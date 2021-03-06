#!/usr/bin/env ruby

require 'csv'
require 'fileutils'
require 'json'
require 'net/http'
require 'pathname'
require 'tempfile'

require 'docopt'
require 'pg'
require 'zip'


DOC = <<-DOCOPT
  Import a GTFS archive into a CitySDK instance.

  Usage:
    import.rb cli [-H HOST | --host=HOST] GTFS_ID GTFS_DIR DATABASE USER
                  PASSWORD
    import.rb database CONFIG_PATH

  Options:
    -H HOST, --host=HOST The host on which the database is running
                         [default: localhost].

DOCOPT


def main(argv = ARGV)
  args = Docopt::docopt(DOC, argv: argv)

  if args.fetch('database')
    database_main(args, 'gtfs')
  else
    cli_main(args)
  end # if

  0
end # def


def database_main(args, gtfs_layer)
  config_path = args.fetch('CONFIG_PATH')
  config = File.open(config_path) do |config_file|
    JSON.load(config_file)
  end # do

  conn = PG::Connection.new(
    host:     config.fetch('db_host'),
    dbname:   config.fetch('db_name'),
    user:     config.fetch('db_user'),
    password: config.fetch('db_pass')
  )

  sql = 'SELECT * FROM gtfs.feed WHERE uri IS NOT NULL;'
  conn.exec(sql).each do |feed|
    uri = URI.parse(feed.fetch('uri'))
    gtfs_id = feed.fetch('gtfs_id')
    last_imported = feed.fetch('last_imported')
    last_imported = Date.parse(last_imported)  unless last_imported.nil?
    download_and_import_gtfs_if_newer(
      conn,
      uri,
      gtfs_id,
      gtfs_layer,
      last_imported
    )
  end # do
end


def download_and_import_gtfs_if_newer(
  conn,
  uri,
  gtfs_id,
  gtfs_layer,
  last_imported
)
  puts 'Checking when the GTFS archive was modified...'
  last_modified = get_last_modified(uri)
  puts "The GTFS archive was modified at #{ last_modified }."

  if last_imported.nil? || last_modified > last_imported
    puts "The GTFS has been modified since #{ last_imported }."
    archive_path = download_and_import_gtfs(
      conn,
      uri,
      gtfs_id,
      gtfs_layer,
      last_modified
    )
  else
    puts "The GTFS has not been modified since #{ last_imported }."
  end # if
end # def


def get_last_modified(uri)
  use_ssl = uri.scheme == 'https'
  http = Net::HTTP.start(uri.host, uri.port, use_ssl: use_ssl)
  response = http.head(uri.request_uri)
  last_modified = response.fetch('Last-Modified')
  last_modified = last_modified[/.*,\s+(.*)\s+\d\d:/, 1]
  Date.parse(last_modified)
end # def


def download_and_import_gtfs(conn, uri, gtfs_id, gtfs_layer, last_modified)
  sql = <<-SQL
    UPDATE gtfs.feed
      SET last_imported = $1::timestamptz
      WHERE gtfs_id = $2::text
    ;
  SQL

  Tempfile.create([gtfs_id, '.zip']) do |archive|
    archive.binmode
    download_gtfs(uri, archive)
    archive.seek(0)
    Dir.mktmpdir do |gtfs_dir|
      gtfs_dir = Pathname.new(gtfs_dir)
      extract_gtfs(archive, gtfs_dir)
      conn.transaction do
        expand_ranges_and_apply_exceptions(gtfs_dir)
        import_gtfs_and_upsert_nodes(conn, gtfs_id, gtfs_dir, gtfs_layer)
        conn.exec_params(sql, [last_modified, gtfs_id])
      end # do
    end # do
  end # do
end # def


def download_gtfs(uri, archive)
  puts 'Downloading GTFS archive...'

  use_ssl = uri.scheme == 'https'
  Net::HTTP.start(uri.host, uri.port, use_ssl: use_ssl) do |http|
    request = Net::HTTP::Get.new(uri)
    http.request(request) do |response|
      response.read_body { |chunk| archive.write(chunk) }
    end # do
  end # do

  puts 'Dowloaded GTFS archive.'
end # def


def extract_gtfs(src, dst)
  Zip::File.open(src) do |zip_file|
    zip_file.each do |f|
      path = dst.join(File.basename(f.name))
      zip_file.extract(f, path)
    end # do
  end # do
end # def


def cli_main(args)
  gtfs_dir = Pathname.new(args.fetch('GTFS_DIR'))
  expand_ranges_and_apply_exceptions(gtfs_dir)

  conn = PG::Connection.new(
    host:     args.fetch('--host'),
    dbname:   args.fetch('DATABASE'),
    user:     args.fetch('USER'),
    password: args.fetch('PASSWORD')
  )

  gtfs_id = args.fetch('GTFS_ID')

  conn.transaction do
    ensure_feed_exists(conn, gtfs_id)
    import_gtfs_and_upsert_nodes(conn, gtfs_id, gtfs_dir, 'gtfs')
  end # do
ensure
  conn.close unless conn.nil?
end # def

def ensure_feed_exists(conn, gtfs_id)
  sql = 'SELECT EXISTS (SELECT NULL FROM gtfs.feed WHERE gtfs_id = $1::text);'
  result = conn.exec_params(sql, [gtfs_id])
  return if result[0].fetch('exists') == 't'
  sql = 'INSERT INTO gtfs.feed (gtfs_id) VALUES ($1::text);'
  conn.exec_params(sql, [gtfs_id])
end # def

def import_gtfs_and_upsert_nodes(conn, gtfs_id, gtfs_dir, gtfs_layer)
  conn.exec('SET search_path TO gtfs;')
  import_gtfs(conn, gtfs_id, gtfs_dir)
  conn.exec('SET search_path TO public;')
  upsert_nodes(conn, gtfs_id, gtfs_layer)
  update_layer_bounds(conn, gtfs_layer)
end # def


# ==============================================================================
# = Preprocess calendar                                                        =
# ==============================================================================

EXCEPTION_TYPE_ADDED = '1'
EXCEPTION_TYPE_REMOVED = '2'


def expand_ranges_and_apply_exceptions(gtfs_dir)
  calendar = gtfs_dir.join('calendar.txt')
  calendar_dates_src = gtfs_dir.join('calendar_dates.txt')
  calendar_dates_dst = gtfs_dir.join('calendar_dates.preprocessed.txt')

  # If the preoprocessed version exists, we don't need to do anything.
  return if calendar_dates_dst.exist?

  exceptions = {}

  CSV.foreach(calendar_dates_src, :headers => true) do |row|
    service_id = row.fetch('service_id')
    date = Date.parse(row.fetch('date'))
    exception_type = row.fetch('exception_type')
    ((exceptions[service_id] ||= {})[service_id] ||= {})[date] = exception_type
  end

  CSV.open(calendar_dates_dst, 'w') do |csv|
    csv << ['service_id', 'date', 'exception_type']

    CSV.foreach(calendar, :headers => true) do |row|
      days = [
        row.fetch('sunday'),
        row.fetch('monday'),
        row.fetch('tuesday'),
        row.fetch('wednesday'),
        row.fetch('thursday'),
        row.fetch('friday'),
        row.fetch('saturday')
      ]

      expand_calendar_row(
        exceptions,
        row.fetch('service_id'),
        days,
        Date.parse(row.fetch('start_date')),
        Date.parse(row.fetch('end_date')),
        csv
      )
    end
  end
end


def expand_calendar_row(exceptions, service_id, days, start_date, end_date, csv)
  date = start_date
  end_date = end_date.next

  while date < end_date do
    has = lambda do |exception_type|
      service_has_exception(exceptions, service_id, date, exception_type)
    end

    runs = false

    # If normally runs, check if it exceptionally doesn't.
    if days[date.wday] == '1'
      unless has.call(EXCEPTION_TYPE_REMOVED)
        runs = true
      end
    elsif has.call(EXCEPTION_TYPE_ADDED)
      # Normally does not run, but this is an exception.
      runs = true
    end

    if runs
      csv << [service_id, date.strftime('%Y%m%d'), 1]
    end

    date = date.next
  end
end


def service_has_exception(exceptions, service_id, date, exception_type)
  if exceptions.key?(service_id)
    service_exceptions = exceptions[service_id]
    if service_exceptions.key?(date)
      if service_exceptions[date] == exception_type
        return true
      end
    end
  end
  false
end


# ==============================================================================
# = Import                                                                     =
# ==============================================================================

def import_gtfs(conn, gtfs_id, gtfs_dir)
  importer = GTFSImporter.new(conn, gtfs_id, gtfs_dir)
  importer.import_all
end


class GTFSImporter
  def initialize(conn, gtfs_id, gtfs_dir)
    @conn = conn
    @gtfs_id = gtfs_id
    @gtfs_dir = gtfs_dir
  end

  def import_all
    import_agency
    import_routes
    import_stops
    import_trips
    import_stop_times
    import_feed_info
    import_calendar_dates
    import_shapes
  end

  def import_agency
    import('agency')
  end

  def import_calendar_dates
    import('calendar_dates', file_name: 'calendar_dates.preprocessed.txt')
  end

  def import_feed_info
    import('feed_info', optional: true)
  end

  def import_routes
    import('routes')
  end

  def import_shapes
    import('shapes', optional: true)
  end

  def import_stops
    import('stops')
  end

  def import_stop_times
    import('stop_times')
  end

  def import_trips
    import('trips')
  end

  private

  def import(base_table_name, options = {})
    file_name = options.fetch(:file_name, "#{ base_table_name }.txt")
    optional = options.fetch(:optional , false)
    path = @gtfs_dir.join(file_name)
    return if optional && !File.exists?(path)
    importer = Importer.new(@conn, @gtfs_id, base_table_name, path)
    importer.import
  end # def
end # class

class Importer
  def initialize(conn, gtfs_id, base_table_name, path)
    @conn = conn
    @gtfs_id = gtfs_id
    @path = path
    @table_name = base_table_name
    @tmp_table_name = 'tmp_' + base_table_name
    @tmp_columns = CSV.open(@path) { |csv| csv.shift() }
  end

  def import()
    puts "Importing #{ @table_name }..."

    puts "\tCreating temporary table"
    create_tmp_table()

    puts "\tCopying csv"
    copies = copy_csv().cmd_tuples

    puts "\tLocking table"
    lock_table()

    puts "\tDeleting old rows"
    deletes = delete_rows().cmd_tuples

    puts "\tInserting new rows"
    inserts = insert_rows().cmd_tuples

    puts "\t#{ copies } copies, #{ deletes } deletes, #{ inserts } inserts"
  end

  private

  def create_tmp_table
    columns = make_columns
    @conn.exec <<-SQL
      CREATE TEMPORARY TABLE #{ @tmp_table_name }
      ON COMMIT DROP
      AS (SELECT #{ columns } FROM #{ @table_name })
      WITH NO DATA;
    SQL
  end

  def copy_csv
    columns = make_columns
    sql = "COPY #{ @tmp_table_name } (#{ columns }) FROM STDOUT CSV HEADER;"
    @conn.copy_data(sql) do
      IO.foreach(@path) { |line| @conn.put_copy_data(line) }
    end
  end

  def lock_table
    @conn.exec("LOCK TABLE #{ @table_name } IN EXCLUSIVE MODE;")
  end

  def delete_rows
    sql = "DELETE FROM #{ @table_name } WHERE gtfs_id = $1::text;"
    @conn.exec_params(sql, [@gtfs_id]);
  end

  def insert_rows
    dst_columns = make_columns
    src_columns = make_columns(table = @tmp_table_name)
    sql = <<-SQL
      INSERT INTO #{ @table_name } (gtfs_id, #{ dst_columns })
        SELECT $1::text, #{ src_columns }
        FROM #{ @tmp_table_name }
      ;
    SQL
    @conn.exec(sql, [@gtfs_id])
  end

  def make_columns(table = nil)
    columns = @tmp_columns.dup
    unless table.nil?
      columns.map! do |column|
        "#{ table }.#{ column }"
      end
    end
    columns.join(', ')
  end
end


# ==============================================================================
# = Nodes                                                                      =
# ==============================================================================

def upsert_nodes(conn, gtfs_id, layer_name)
  layer_id = find_layer_id_from_name(conn, layer_name)
  upsert_stop_nodes_and_stop_node_data(conn, gtfs_id, layer_id)
  upsert_route_nodes(conn, gtfs_id, layer_id)
end


def find_layer_id_from_name(conn, layer_name)
  sql = 'SELECT id FROM layers WHERE name = $1::text;'
  conn.exec_params(sql, [layer_name])[0].fetch('id')
end


# = Stop nodes =================================================================

def upsert_stop_nodes_and_stop_node_data(conn, gtfs_id, layer_id)
  puts 'Upserting stop nodes and stop data nodes...'

  sql = 'SELECT * FROM gtfs.stops WHERE gtfs_id = $1::text;'
  conn.exec_params(sql, [gtfs_id]).each do |stop|
    node_id = upsert_stop_node(conn, stop, layer_id)
    upsert_stop_node_data(conn, stop, layer_id, node_id)
  end

  puts 'Upserted stop nodes and stop data nodes'
end


def upsert_stop_node(conn, stop, layer_id)
  cdkid_suffix = stop.fetch('stop_id').downcase.gsub(/\W/, '.')
  cdkid = 'gtfs.stop.' + cdkid_suffix

  sql = 'SELECT id FROM nodes WHERE cdk_id = $1::text;'
  result = conn.exec_params(sql, [cdkid])

  if result.cmd_tuples.zero?
    node_id = insert_stop_node(conn, stop, layer_id, cdkid)
  else
    node_id = result[0].fetch('id')
    update_stop_node(conn, stop, node_id)
  end

  node_id
end

def insert_stop_node(conn, stop, layer_id, cdkid)
  sql = <<-SQL
    INSERT
      INTO nodes (
        cdk_id,
        layer_id,
        node_type,
        name,
        geom
      )
      VALUES (
        $1::text,    -- cdk_id
        $2::integer, -- layer_id
        2,           -- node_type
        $3::text,    -- name
        ST_SetSRID(  -- geom
          ST_Point(
            $4::double precision, -- x_lon
            $5::double precision  -- y_lat
          ),
          4326
        )
      )
      RETURNING id
    ;
  SQL
  result = conn.exec_params(sql, [
    cdkid,
    layer_id,
    stop.fetch('stop_name'),
    stop.fetch('stop_lon'),
    stop.fetch('stop_lat')
  ])
  result[0].fetch('id')
ensure
  result.clear unless result.nil?
end # def

def update_stop_node(conn, stop, node_id)
  sql = <<-SQL
    UPDATE nodes
      SET
        name = $1::text,
        geom = ST_SetSRID(
          ST_Point(
            $2::double precision, -- x_lon
            $3::double precision  -- y_lat
          ),
          4326
        )
      WHERE id = $4::integer
    ;
  SQL

  conn.exec_params(sql, [
    stop.fetch('stop_name'),
    stop.fetch('stop_lon'),
    stop.fetch('stop_lat'),
    node_id
  ])
end


# = Stop node data =============================================================

def upsert_stop_node_data(conn, stop, layer_id, node_id)
  sql = <<-SQL
    SELECT id
      FROM node_data
      WHERE
        layer_id = $1::integer
        AND node_id = $2::integer
    ;
  SQL

  result = conn.exec_params(sql, [layer_id, node_id])
  data = hash_to_hstore(conn, stop)
  modalities = get_modalities_for_stop(conn, stop)

  if result.cmd_tuples.zero?
    node_data_id = insert_stop_node_data(conn, stop, layer_id, node_id)
  else
    node_data_id = result[0].fetch('id')
    update_stop_node_data(conn, stop, node_data_id)
  end

  node_data_id
end


def insert_stop_node_data(conn, stop, layer_id, node_id)
  sql = <<-SQL
    INSERT
      INTO node_data (
        layer_id,
        node_id,
        data,
        modalities
      )
      VALUES (
        $1::integer,  -- layer_id
        $2::integer,  -- node_id
        $3::hstore,   -- data
        $4::integer[] -- modalities
      )
      RETURNING id
    ;
  SQL
  result = conn.exec_params(sql, [
    layer_id,
    node_id,
    hash_to_hstore(conn, stop),
    get_modalities_for_stop(conn, stop)
  ])
  result[0].fetch('id')
ensure
  result.clear unless result.nil?
end


def update_stop_node_data(conn, stop, node_data_id)
  sql = <<-SQL
    UPDATE node_data
      SET
        data = $1::hstore,
        modalities = $2::integer[]
      WHERE id = $3::integer
    ;
  SQL
  conn.exec_params(sql, [
    hash_to_hstore(conn, stop),
    get_modalities_for_stop(conn, stop),
    node_data_id
  ])
end


# = Route nodes ================================================================

def upsert_route_nodes(conn, gtfs_id, layer_id)
  puts 'Upserting route nodes...'

  sql = <<-SQL
    SELECT feed_valid_from, feed_valid_to
    FROM gtfs.feed_info
    WHERE gtfs_id = $1::text;
  SQL

  result = conn.exec_params(sql, [gtfs_id])
  validity = nil

  if result.cmd_tuples > 0
    feed = result[0]

    # TFGM uses the wrong field names. feed_(start|end)_data are the
    # correct headers.

    start_date_key =
      if feed.key?('feed_valid_from')
        'feed_valid_from'
      else
        'feed_start_date'
      end # else

    end_date_key =
      if feed.key?('feed_valid_to')
        'feed_valid_to'
      else
        'feed_end_date'
      end # else

    start_date = feed.fetch(start_date_key)
    end_date = feed.fetch(end_date_key)
    validity = "[#{ start_date } 00:00, #{ end_date } 23:59]"
  end # if

  result.clear

  sql = <<-SQL
    SELECT *
      FROM gtfs.routes
      WHERE gtfs_id = $1::text
    ;
  SQL

  result = conn.exec_params(sql, [gtfs_id])
  num_routes_to_add = result.cmdtuples

  result.each do |route|
    puts [
      num_routes_to_add,
      route.fetch('route_id'),
      route.fetch('route_short_name'),
      route.fetch('route_long_name')
    ].join('; ')

    [0, 1].each do |direction|
        upsert_route(conn, gtfs_id, layer_id, route, direction, validity)
    end

    num_routes_to_add -= 1
  end # do
end


def upsert_route(conn, gtfs_id, layer_id, route, direction, validity)
  sql = 'SELECT * FROM gtfs.stops_for_line($1::text, $2::integer);'
  route_id = route.fetch('route_id')
  stops = conn.exec_params(sql, [route_id, direction])

  return if stops.cmd_tuples == 0

  members = []
  line = []
  start_name = end_name = nil

  sql = 'SELECT * FROM shape_for_line($1::text);'
  shape = conn.exec(sql, [route_id])
  found_shape = shape.cmd_tuples >= 0

  stops.each do |stop|
    stop_id = stop.fetch('stop_id')
    next if stop_id.nil? || stop_id == ''

    sql = <<-SQL
      SELECT *
        FROM node_data
        WHERE
          layer_id = $1::int
          AND (data @> $2::hstore)
      ;
    SQL

    result = conn.exec_params(sql, [
      layer_id,
      hash_to_hstore(conn,
        'gtfs_id' => gtfs_id,
        'stop_id' => stop_id
      )
    ])

    if result.cmd_tuples > 0
      result.each do |node_data|
        start_name = stop.fetch('name')
        end_name = stop.fetch('name')
        node_data_str = node_data.fetch('node_id')
        members << node_data_str

        if found_shape
          sql = 'SELECT geom FROM nodes WHERE id = $1::integer;'
          result = conn.exec_params(sql, [node_data_str])
          geom = result[0].fetch('geom')
          line << "'#{ geom }'::geometry"
        end # if

      end # do
    end # if
  end # do

  if members.length > 0
    if found_shape
      shape.each do |shape|
        geom = shape.fetch('geom')
        line << "'#{ geom }'"
      end # do
    end # if

    route['route_from'] = start_name
    route['route_to'] = end_name

    node_id = upsert_route_node(
      conn,
      route,
      members,
      line,
      direction,
      gtfs_id,
      layer_id
    )

    upsert_route_node_data(conn, route, layer_id, node_id, validity)
  end # if
end


def upsert_route_node(conn, route, members, line, direction, gtfs_id, layer_id)
  sql = 'SELECT id FROM nodes WHERE cdk_id = $1::text;'

  short_name = route.fetch('route_short_name')

  cdk_id = [
    'gtfs',
    'line',
    gtfs_id.gsub(/\W/, ''),
    "#{ short_name.gsub(/\W/, '') }-#{ direction }".downcase
  ].join('.')

  result = conn.exec_params(sql, [cdk_id])

  name = [
    gtfs_id,
    get_modality_name_for_route(route),
    short_name
  ].join(' ')

  members = "{#{ members.join(',') }}"

  line = "ST_MakeLine(ARRAY[#{ line.join(',') }])"

  if result.cmd_tuples == 0
    route_node_id = insert_route_node(
      conn,
      layer_id,
      cdk_id,
      name,
      members,
      line
    )
  else
    route_node_id = result[0].fetch('id')
    update_route_node(conn, route_node_id, name, members, line)
  end

  route_node_id
end


def insert_route_node(conn, layer_id, cdk_id, name, members, line)
  sql = <<-SQL
    INSERT
      INTO nodes (
        name,
        cdk_id,
        layer_id,
        node_type,
        members,
        geom
      )
      VALUES (
        $1::text,     -- name
        $2::text,     -- cdk_id
        $3::integer,  -- layer_id
        3,            -- node_type
        $4::bigint[], -- members
        #{ line }     -- geom
      )
      RETURNING id
    ;
  SQL
  result = conn.exec_params(sql, [
    name,
    cdk_id,
    layer_id,
    members,
  ])
  result[0].fetch('id')
ensure
  result.clear unless result.nil?
end


def update_route_node(conn, route_node_id, name, members, line)
  sql = <<-SQL
    UPDATE nodes
      SET
        name = $1::text,
        geom = #{ line },
        members = $2::bigint[]
      WHERE id = $3::integer
    ;
  SQL
  conn.exec_params(sql, [name, members, route_node_id])
end


def upsert_route_node_data(conn, route, layer_id, node_id, validity)
  sql = <<-SQL
    SELECT id
      FROM node_data
      WHERE
        layer_id = $1::integer
        AND node_id = $2::integer
    ;
  SQL
  result = conn.exec_params(sql, [layer_id, node_id])
  if result.cmd_tuples == 0
    node_data_id = insert_route_node_data(
      conn,
      route,
      layer_id,
      node_id,
      validity
    )
  else
    node_data_id = result[0].fetch('id')
    update_route_node_data(conn, route, node_data_id, validity)
  end
  node_data_id
end


def insert_route_node_data(conn, route, layer_id, node_id, validity)
  sql = <<-SQL
    INSERT
      INTO node_data (
        node_id,
        layer_id,
        data,
        modalities,
        validity
      )
      VALUES (
        $1::integer,   -- node_id
        $2::integer,   -- layer_id
        $3::hstore,    -- data
        $4::integer[], -- modalities
        $5::tstzrange  -- validity
      )
      RETURNING id
    ;
  SQL
  result = conn.exec_params(sql, [
    node_id,
    layer_id,
    hash_to_hstore(conn, route),
    '{' + route.fetch('route_type') + '}',
    validity
  ])
  result[0].fetch('id')
end


def update_route_node_data(conn, route, node_data_id, validity)
  sql = <<-SQL
    UPDATE node_data
      SET
        data = $1::hstore,
        modalities = $2::integer[],
        validity = $3::tstzrange
      WHERE id = $4::integer
    ;
  SQL

  modality = route.fetch('route_type')
  conn.exec_params(sql, [
    hash_to_hstore(conn, route),
    "{#{ modality }}",
    validity,
    node_data_id
  ])
end

def update_layer_bounds(conn, layer_name)
  sql = 'SELECT update_layer_bounds($1::integer);'
  layer_id = find_layer_id_from_name(conn, layer_name)
  result = conn.exec_params(sql, [layer_id])
  return # nothing
ensure
  result.clear() unless result.nil?
end # ensure


# = Helpers ====================================================================

def get_modality_name_for_route(route)
  modality = route.fetch('route_type').to_i
  case modality
  when 0 then 'tram'
  when 1 then 'subway'
  when 2 then 'rail'
  when 3 then 'bus'
  when 4 then 'ferry'
  when 5 then 'cable car'
  when 6 then 'gondola'
  when 7 then 'funicular'
  else fail "#{ modality } is not a modality."
  end
end


def get_modalities_for_stop(conn, stop)
  sql = 'SELECT DISTINCT type FROM gtfs.lines_for_stop($1::text);'
  stop_id = stop.fetch('stop_id')
  result = conn.exec_params(sql, [stop_id])
  modalities = []
  result.each do |line|
    modality_name = line.fetch('type')
    modality =
      case modality_name
      when 'Tram'      then 0
      when 'Subway'    then 1
      when 'Rail'      then 2
      when 'Bus'       then 3
      when 'Ferry'     then 4
      when 'Cable car' then 5
      when 'Gondola'   then 6
      when 'Funicular' then 7
      else fail "#{ modality_name } is not a modality name."
      end
    modalities << modality
  end
  modalities = modalities.join(',')
  "{#{ modalities }}"
end # def

def hash_to_hstore(conn, hash)
  escape = lambda { |obj| '"' + conn.escape(obj) + '"' }
  pairs = []
  hash.each_pair do |key, value|
    next if value.nil?
    key = escape.call(key)
    value = escape.call(value)
    pairs << "#{ key } => #{ value }"
  end
  pairs.join(',')
end # def

if __FILE__ == $0
  exit main
end

