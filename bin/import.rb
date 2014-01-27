#!/usr/bin/env ruby

require 'csv'
require 'getoptlong'
require 'json'
require 'pg'
require 'socket'
require 'tempfile'
require 'citysdk/importers/gtfs/util.rb'

$quote = '"'

$gtfs_files = [
  'agency',
  'calendar_dates',
  'feed_info',
  'routes',
  'shapes',
  'stop_times',
  'stops',
  'trips'
]

def create_schema(conn)
  conn.exec('DROP SCHEMA IF EXISTS igtfs CASCADE;')
  conn.exec('CREATE SCHEMA igtfs;')
end

def get_columns(data_dir, name)
  path = File.join(data_dir, "#{name}.txt")
  File.open(path) do |csv|
    csv.gets.strip.split(',')
  end
end

$c_types = {

  'shapes' => {
    'shape_id' => 'text',
    'shape_pt_lat' => 'text',
    'shape_pt_lon' => 'text',
    'shape_pt_sequence' => 'integer'
  },

  'feed_info' => {
    'feed_publisher_name' => 'text',
    'feed_publisher_url' => 'text',
    'feed_lang' => 'text',
    'feed_start_date' => 'date',
    'feed_end_date' => 'date',
    'feed_valid_from' => 'date',
    'feed_valid_to' => 'date',
    'feed_version' => 'text'
  },

  'agency' => {
    'agency_id' => 'text primary key',
    'agency_name' => 'text',
    'agency_url' => 'text',
    'agency_timezone' => 'text',
    'agency_lang' => 'text'
  },

  'calendar_dates' => {
    'service_id' => 'text',
    'date' => 'date',
    'exception_type' => 'smallint'
  },

  'stops' => {
    'stop_id' => 'text primary key',
    'stop_name' => 'text',
    'location_type' => 'smallint',
    'parent_station' => 'text',
    'wheelchair_boarding' => 'smallint',
    'platform_code' => 'text',
    'stop_lat' => 'float',
    'stop_lon' => 'float'
  },

  'routes' => {
    'route_id' => 'text primary key',
    'agency_id' => 'text',
    'route_short_name' => 'text',
    'route_long_name' => 'text',
    'route_type' => 'smallint'
  },

  'trips' => {
    'route_id' => 'text',
    'service_id' => 'text',
    'trip_id' => 'text',
    'trip_headsign' => 'text',
    'shape_id' => 'text',
    'direction_id' => 'smallint',
    'wheelchair_accessible' => 'smallint',
    'trip_bikes_allowed' => 'smallint'
  },

  'stop_times' => {
    'trip_id' => 'text',
    'arrival_time' => 'text',
    'departure_time' => 'text',
    'stop_id' => 'text',
    'stop_sequence' => 'smallint',
    'stop_headsign' => 'text',
    'pickup_type' => 'smallint',
    'drop_off_type' => 'smallint'
  }
}


def calendar_dates(conn, data_dir, prefix)
  one_file(conn, data_dir, 'calendar_dates')
  puts 'Merging calendar_dates...'

  conn.exec('DROP INDEX IF EXISTS calendar_dates_service_id;')

  conn.exec <<-SQL
    INSERT INTO gtfs.calendar_dates
    SELECT ('#{prefix}' || service_id), date, exception_type
    FROM igtfs.calendar_dates;
  SQL

  conn.exec(
    'CREATE TABLE calendar_dates AS SELECT DISTINCT * FROM gtfs.calendar_dates;'
  )
  conn.exec('DROP TABLE gtfs.calendar_dates;')
  conn.exec('ALTER TABLE calendar_dates SET SCHEMA gtfs;')
  conn.exec(
    'CREATE INDEX calendar_dates_service_id ON gtfs.calendar_dates(service_id);'
  )
end


def stops(conn, data_dir, prefix)
  columns = one_file(conn, data_dir, 'stops')

  puts 'Merging stops...'

  select = "i.stop_name"
  select += columns.include?('location_type') ? ",i.location_type" : ", 0"
  select += columns.include?('parent_station') ? ",i.parent_station" : ", ''"
  select += columns.include?('platform_code') ? ", i.platform_code" : ", ''"
  select += ",i.location"
  select += columns.include?('wheelchair_boarding') ? ", i.wheelchair_boarding" : ", 0"

  select2 = "stop_id,stop_name,location_type,parent_station, wheelchair_boarding, platform_code,location"
  select2.gsub!('location_type',"0") if !columns.include?('location_type')
  select2.gsub!('parent_station',"''") if !columns.include?('parent_station')
  select2.gsub!('wheelchair_boarding',"0") if !columns.include?('wheelchair_boarding')
  select2.gsub!('platform_code',"''") if !columns.include?('platform_code')

  conn.exec('ALTER TABLE igtfs.stops ADD COLUMN location geometry(point,4326)')
  conn.exec(
    "UPDATE igtfs.stops SET (stop_id, location) = ('#{prefix}' || stop_id, " \
    'ST_SetSRID(ST_Point(stop_lon,stop_lat),4326))'
  )

  if columns.include?('parent_station')
    conn.exec(
      "UPDATE igtfs.stops SET parent_station = '' WHERE parent_station IS NULL"
    )
  end

  if columns.include?('wheelchair_boarding')
    conn.exec(
      'UPDATE igtfs.stops SET wheelchair_boarding = 0 ' \
      'WHERE wheelchair_boarding IS NULL'
    )
  end

  if columns.include?('platform_code')
    conn.exec(
      "UPDATE igtfs.stops SET platform_code = '' WHERE platform_code IS NULL"
    )
  end

  if columns.include?('location_type')
    conn.exec(
      'UPDATE igtfs.stops SET location_type = 0 WHERE location_type IS NULL'
    )
  end

  conn.exec <<-SQL
    WITH upsert AS (
      UPDATE gtfs.stops g SET (
        stop_name,
        location_type,
        parent_station,
        platform_code,
        location,
        wheelchair_boarding
      ) = (#{select})
      FROM igtfs.stops i WHERE g.stop_id = i.stop_id
      RETURNING g.stop_id
    )
    INSERT INTO gtfs.stops
      SELECT #{select2}
      FROM igtfs.stops a
      WHERE a.stop_id NOT IN (SELECT stop_id FROM upsert);
  SQL
end

def agency(conn, data_dir)
  columns = one_file(conn, data_dir, 'agency')
  return unless columns
  puts "Merging agency.."

  select  = "agency_id,agency_name,agency_url,agency_timezone,agency_lang"
  iselect = "i.agency_name,i.agency_url,i.agency_timezone,i.agency_lang"

  if !columns.include?('agency_lang')
    select.gsub!('agency_lang', "''")
    iselect.gsub!('i.agency_lang', "''")
  end

  conn.exec <<-SQL
    WITH upsert AS (
      UPDATE gtfs.agency g SET (
        agency_name,
        agency_url,
        agency_timezone,
        agency_lang
      ) = (#{iselect})
      FROM igtfs.agency i WHERE g.agency_id = i.agency_id
      RETURNING g.agency_id
    )
    INSERT INTO gtfs.agency
    SELECT #{select}
    FROM igtfs.agency a
    WHERE a.agency_id NOT IN (
      SELECT agency_id FROM upsert
    );
  SQL
end

def shapes(conn, data_dir)
  unless File.file?(File.join(data_dir, 'shapes.txt'))
    return
  end

  puts 'Merging shapes...'
  columns = one_file(conn, data_dir, 'shapes')

  conn.exec <<-SQL
    DELETE FROM gtfs.shapes g
      WHERE g.shape_id IN (
        SELECT DISTINCT shape_id FROM igtfs.shapes
      );

    INSERT INTO gtfs.shapes
      SELECT shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence
      FROM igtfs.shapes;
  SQL
end

def feed_info(conn, data_dir)
  unless File.file?(File.join(data_dir, 'feed_info'))
    return
  end

  columns = one_file(conn, data_dir, 'feed_info')
  return unless columns

  a = []
  CSV.foreach(
    File.join(data_dir, 'agency.txt'),
    :quote_char => '"',
    :col_sep =>',',
    :headers => true,
    :row_sep =>:auto
  ) do |row|
    a << row['agency_name']
  end
  a = conn.escape(a.join(', '))

  select = [
    'feed_publisher_name',
    'feed_publisher_url',
    'feed_lang',
    'feed_start_date',
    'feed_end_date',
    'feed_version',
    a,
    Time.now.strftime('%Y-%m-%d')
  ].join(',')

  if columns.include?('feed_valid_to')
    select.gsub!('feed_end_date', 'feed_valid_to')
  end

  if columns.include?('feed_valid_from')
    select.gsub!('feed_start_date', 'feed_valid_from')
  end

  conn.exec <<-SQL
    DELETE FROM gtfs.feed_info g USING igtfs.feed_info i
      WHERE g.feed_version = i.feed_version;
    INSERT INTO gtfs.feed_info select #{select} FROM igtfs.feed_info;
  SQL
end

def routes(conn, data_dir, prefix)
  columns = one_file(conn, data_dir, 'routes')
  puts "Merging routes.."

  select  = "route_id,agency_id,route_short_name,route_long_name,route_type"
  iselect = "i.agency_id,i.route_short_name,i.route_long_name,i.route_type"
  unless columns.include?('agency_id')
    select.gsub!('agency_id', "''")
    iselect.gsub!('i.agency_id', "''")
  end

  conn.exec("UPDATE igtfs.routes SET (route_id) = ('#{prefix}' || route_id)")

  conn.exec <<-SQL
    UPDATE igtfs.routes
    SET route_short_name = route_long_name
    WHERE route_short_name is NULL
  SQL

  conn.exec <<-SQL
    WITH upsert as
    (update gtfs.routes g set
      (agency_id,route_short_name,route_long_name,route_type) =
      (#{iselect})
      from igtfs.routes i where g.route_id=i.route_id
        returning g.route_id
    )
    insert into gtfs.routes select
      #{select}
    from igtfs.routes a where a.route_id not in (select route_id from upsert);
  SQL
end

def stop_times(conn, data_dir, prefix)
  columns = one_file(conn, data_dir, 'stop_times')

  conn.exec('DROP INDEX IF EXISTS gtfs.stop_times_trip_id;')
  conn.exec('DROP INDEX IF EXISTS gtfs.stop_times_stop_id;')
  conn.exec('DROP INDEX IF EXISTS gtfs.stop_times_departure_time;')
  conn.exec('DROP INDEX IF EXISTS gtfs.stop_times_stop_id_trip_id;')

  select = (
    "'#{prefix}' || trip_id, arrival_time, departure_time, "
    "'#{prefix}' || stop_id, stop_sequence, stop_headsign, "
    "pickup_type, drop_off_type"
  )

  select.gsub!('stop_headsign', "''") unless columns.include?('stop_headsign')
  select.gsub!('pickup_type', '0') unless columns.include?('pickup_type')
  select.gsub!('drop_off_type', '0') unless columns.include?('drop_off_type')

  puts 'Merging stop_times...'
  conn.exec(
    "INSERT INTO gtfs.stop_times SELECT #{select} FROM igtfs.stop_times;"
  )

  conn.exec(
    'CREATE TABLE stop_times AS SELECT DISTINCT * FROM gtfs.stop_times;'
  )
  conn.exec('DROP TABLE gtfs.stop_times;')
  conn.exec('ALTER TABLE stop_times SET SCHEMA gtfs;')

  puts 'Creating index on stop_times(stop_id,trip_id)...'
  conn.exec <<-SQL
    CREATE INDEX stop_times_stop_id_trip_id
    ON gtfs.stop_times
    USING btree(trip_id, stop_id);
  SQL

  puts 'Creating index on stop_times(trip_id)...'
  conn.exec('CREATE INDEX stop_times_trip_id ON gtfs.stop_times(trip_id);')

  puts 'Creating index on stop_times(stop_id)...'
  conn.exec('CREATE INDEX stop_times_stop_id ON gtfs.stop_times(stop_id);')

  puts 'Creating index on stop_times(departure_time)...'
  conn.exec(
    'CREATE INDEX stop_times_departure_time ON gtfs.stop_times(departure_time);'
  )
end

def trips(conn, data_dir, prefix)
  columns = one_file(conn, data_dir, 'trips')
  return unless columns

  puts "Merging trips.."

  select = "'#{prefix}' || route_id, '#{prefix}' || service_id, '#{prefix}' || trip_id"
  select += columns.include?('trip_headsign') ? ", trip_headsign" : ", ''"
  select += columns.include?('direction_id') ? ", direction_id" : ", 0"
  select += columns.include?('wheelchair_accessible') ? ", wheelchair_accessible" : ", 0"
  select += columns.include?('trip_bikes_allowed') ? ", trip_bikes_allowed" : ", 0"
  select += columns.include?('shape_id') ? ", '#{prefix}' || shape_id" : ", ''"

  if columns.include?('direction_id')
    conn.exec(
      'UPDATE igtfs.trips SET direction_id = 0 WHERE direction_id is NULL'
    )
  end

  conn.exec("INSERT INTO gtfs.trips SELECT #{select} FROM igtfs.trips;")
  conn.exec('CREATE TABLE trips as SELECT DISTINCT * FROM gtfs.trips;')
  conn.exec('DROP TABLE gtfs.trips;')
  conn.exec('ALTER TABLE trips SET SCHEMA gtfs;')
  conn.exec('CREATE INDEX trips_trip_id ON gtfs.trips(trip_id);')
  conn.exec('CREATE INDEX trips_route_id ON gtfs.trips(route_id);')
  conn.exec('CREATE INDEX trips_direction_id ON gtfs.trips(direction_id);')
end

def one_file(conn, data_dir, name)
  puts "Copying #{name} from disk..."

  columns = get_columns(data_dir, name)
  return unless columns

  ca = columns.map do |column|
    column_type = $c_types[name][column] || 'text'
    "#{column} #{column_type}"
  end

  ca = ca.join(',')
  conn.exec("CREATE table igtfs.#{name} (#{ca})")
  conn.exec("COPY igtfs.#{name} FROM stdin QUOTE '#{$quote}' CSV HEADER")

  path = File.join(data_dir, "#{name}.txt")
  File.open(path, 'r').each_line do |line|
     fail unless conn.put_copy_data line
  end

  fail unless conn.put_copy_end
  columns
end

def copy_tables(conn, data_dir, prefix)
  create_schema(conn)
  feed_info(conn, data_dir)
  agency(conn, data_dir)
  calendar_dates(conn, data_dir, prefix)
  routes(conn, data_dir, prefix)
  trips(conn, data_dir, prefix)
  stops(conn, data_dir, prefix)
  stop_times(conn, data_dir, prefix)
  shapes(conn, data_dir)
end

def do_stops(conn, layer_id, data_dir, prefix)
  puts 'Mapping stops..'

  stops_a = []

  CSV.open(
    File.join(data_dir, 'stops.txt'),
    'r:bom|utf-8',
    :quote_char => '"',
    :col_sep =>',',
    :headers => true,
    :row_sep => :auto
  ) do |csv|
    csv.each do |row|
      s = prefix + row['stop_id']
      stops_a << "'#{s}'"
    end
  end

  stops_a_csv = stops_a.join(',')
  stops = conn.exec(
    "SELECT * FROM gtfs.stops WHERE stop_id IN (#{stops_a_csv}) " \
    'ORDER BY stop_name'
  );
  stopsCount = stops.cmdtuples

  stops.each do |stop|
    nid = GTFS_Import::stopNode(conn, layer_id, stop)
    GTFS_Import::stopNodeData(conn, layer_id, nid, stop) if nid
    if stopsCount % 100 == 0
      puts "#{stopsCount}; #{stop['stop_name']}"
    end
    stopsCount -= 1
  end
end

def do_routes(conn, layer_id, data_dir, prefix)
  puts 'Mapping routes...'

  @val=nil

  path = File.join(data_dir, 'feed_info.txt')
  if File.file?(path)
    File.open(path) do |f|
      headers = f.gets.chomp.split(',')
      sd = headers.index('feed_start_date') || headers.index('feed_valid_from')
      ed = headers.index('feed_end_date') || headers.index('feed_valid_to')
      if sd && ed
        s = f.gets.chomp.split(',')
        sd = s[sd] + " 00:00"
        ed = s[ed] + " 23:59"
        @val = "[#{sd},#{ed}]"
      end
    end
  end

  $routes_rejected = 0

  route_a = CSV.open(
    File.join(data_dir, 'routes.txt'),
    'r:bom|utf-8',
    :quote_char => '"',
    :col_sep =>',',
    :headers => true,
    :row_sep =>:auto
  ) do |csv|
    csv.map do |row|
      "'#{prefix}#{row['route_id']}'"
    end
  end

  route_a_csv = route_a.join(',')
  routes = conn.exec(
    "SELECT * FROM gtfs.routes WHERE route_id IN (#{route_a_csv})"
  )

  num_routes_to_add = routes.cmdtuples

  routes.each do |route|
    puts [
      num_routes_to_add,
      route['route_id'],
      route['route_short_name'],
      route['route_long_name']
    ].join('; ')

    num_routes_to_add -= 1
    GTFS_Import::addOneRoute(conn, layer_id, route, 0, @val, prefix)
    GTFS_Import::addOneRoute(conn, layer_id, route, 1, @val, prefix)
  end
end

def do_cleanup(conn)
  puts 'Cleaning up...'

  puts 'Collecting old trips..'
  conn.exec <<-SQL
    SELECT
      trip_id,
      service_id
    INTO TEMPORARY cu_tripids
    FROM gtfs.trips
    WHERE service_id in (
      SELECT DISTINCT service_id
      FROM gtfs.calendar_dates
      WHERE date <= (now() - '2 days'::interval)
    );
  SQL

  puts 'Removing still valid trips from collection...'
  conn.exec <<-SQL
    DELETE FROM cu_tripids
    WHERE service_id IN (
      SELECT DISTINCT service_id
      FROM gtfs.calendar_dates
      WHERE date > (now() - '2 days'::interval)
    );
  SQL

  puts 'Deleting old stoptimes...'
  conn.exec <<-SQL
    DELETE FROM gtfs.stop_times
    WHERE trip_id IN (
      SELECT DISTINCT trip_id
      FROM cu_tripids
    );
  SQL

  puts 'Deleting obsolete trips...'
  conn.exec <<-SQL
    DELETE FROM gtfs.trips
    WHERE trip_id IN (
      SELECT DISTINCT trip_id
      FROM cu_tripids
    );
  SQL

  puts 'Deleting old calendar date entries...'
  conn.exec <<-SQL
    DELETE FROM gtfs.calendar_dates
    WHERE DATE <= (now() - '2 days'::interval);
  SQL
end

def main(argv)
  opts = GetoptLong.new(
    ['--prefix', '-p', GetoptLong::REQUIRED_ARGUMENT]
  )

  prefix = ''
  opts.each do |opt, arg|
    case opt
      when '--prefix'
        prefix = arg
    end
  end

  database = argv[0]
  user = argv[1]
  password = argv[2]
  data_dir = File.expand_path(argv[3])

  unless File.directory?(data_dir)
    puts "GTFS data directory missing, aborting"
    return 1
  end

  unless database && user && password
    puts "Database credentials missing, aborting"
    return 1
  end

  optional = ['feed_info', 'shapes']

  $gtfs_files.each do |name|
    path = File.join(data_dir, "#{name}.txt")

    unless File.exists?(path)
      if optional.include?(name)
        $gtfs_files.delete(name)
      else
        puts "Bad or incomplete GTFS data set in #{data_dir}, aborting!"
        return 1
      end
    end
  end

  conn = PGconn.new('localhost', '5432', nil, nil, database, user, password)

  res = conn.exec("SELECT id FROM layers WHERE name = 'gtfs'");
  if res.cmdtuples == 0
    puts 'No GTFS layer found, aborting!'
    return 1
  end

  layer_id = res[0]['id'].to_i

  puts "Starting update: prefix: #{prefix}"
  conn.transaction do
    GTFS_Import::cons_calendartxt
    copy_tables(conn, data_dir, prefix)
  end

  puts "\tCommitted copy gtfs."
  conn.transaction do
    do_cleanup(conn)
  end

  puts "\tCommitted cleanup."
  conn.transaction do
    do_stops(conn, layer_id, data_dir, prefix)
    do_routes(conn, layer_id, data_dir, prefix)
  end

  puts 'Committed stops and routes mapping.'

  return 0
ensure
  conn.close unless conn.nil?
end

if __FILE__ == $0
  exit main(ARGV)
end

