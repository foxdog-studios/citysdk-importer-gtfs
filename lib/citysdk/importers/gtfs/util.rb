require 'date'
require 'socket'

module GTFS_Import

  RevMods = {
    '0' => 'tram',
    '1' => 'subway',
    '2' => 'rail',
    '3' => 'bus',
    '4' => 'ferry',
    '5' => 'cable car',
    '6' => 'gondola',
    '7' => 'funicular'
  }

  Modalities = {
    'Tram'  => 0,
    'Subway'  => 1,
    'Rail'  => 2,
    'Bus'  => 3,
    'Ferry'  => 4,
    'Cable car'  => 5,
    'Gondola'  => 6,
    'Funicular'  => 7
  }

  def self.do_log(s)
    $stderr.puts "#{Time.now.strftime('%Y-%m-%d - %H:%M:%S')} -- #{s}"
  end

  $calendar_excepts = {}

  def self.does_run(s,d)
    if $calendar_excepts[s]
      $calendar_excepts[s].each do |e|
        return false if e[0] == d and e[1] == '2'
      end
    end
    true
  end

  def self.does_not_run(s,d)
    if $calendar_excepts[s]
      $calendar_excepts[s].each do |e|
        return false if e[0] == d and e[1] == '1'
      end
    end
    true
  end

  def self.do_one_calendar_row(service_id,days, s, e, f)
    day = s
    end_d = e.next

    while day != end_d do
      if( days[day.wday] == '1' )
        if GTFS_Import::does_run(service_id,day)
          f.puts "#{service_id},#{day.strftime('%Y%m%d')},1"
        end
      else # no service
        if !GTFS_Import::does_not_run(service_id,day) # runs after all...
          f.puts "#{service_id},#{day.strftime('%Y%m%d')},1"
        end
      end
      day = day.next
    end
  end



  # consolidate calendar.txt into flat file calendar_dates.txt with just '1' exception types.
  def self.cons_calendartxt

    if File.exists? "#{$newDir}/calendar.txt" and !File.exists? "#{$newDir}/calendar_dates.txt.old"

      $zrp.p "Consolidating calendar.txt.\n"

      if File.exists? "#{$newDir}/calendar_dates.txt"

        CSV.open("#{$newDir}/calendar_dates.txt", 'r:bom|utf-8', :quote_char => '"', :col_sep =>',',:headers => true, :row_sep =>:auto) do |csv|
          csv.each do |row|
            $calendar_excepts[row['service_id']] = [] if $calendar_excepts[row['service_id']].nil?
            $calendar_excepts[row['service_id']] << [Date.parse(row['date']),row['exception_type']]
          end
        end
        system "mv #{$newDir}/calendar_dates.txt #{$newDir}/calendar_dates.txt.old"
      else
        system "touch #{$newDir}/calendar_dates.txt.old"
      end

      File.open("#{$newDir}/calendar_dates.txt",'w') do |fd|
        fd.puts "service_id,date,exception_type"

        CSV.open("#{$newDir}/calendar.txt", 'r:bom|utf-8', :quote_char => '"', :col_sep =>',',:headers => true, :row_sep =>:auto) do |csv|
          csv.each do |row|
            days = [row['sunday'],row['monday'],row['tuesday'],row['wednesday'],row['thursday'],row['friday'],row['saturday']]
            GTFS_Import::do_one_calendar_row(row['service_id'],days,Date.parse(row['start_date']),Date.parse(row['end_date']), fd)
          end
        end
      end
    end
    $calendar_excepts = {}
  end


  def self.makeCDKID(s)
    Digest::MD5.hexdigest(s).to_i(16).base62_encode
  end

  class ::Hash
    def to_hstore(conn)
      pairs = []
      self.each_pair do |key, value|
        pairs << "['#{conn.escape(key)}','#{conn.escape(value)}']"
      end
      "hstore(ARRAY[#{pairs.join(',')}])"
    end
  end


  def self.modalitiesForStop(conn, s)
    mods = []
    stop_id = s['stop_id']
    lines = conn.exec("select * from lines_for_stop('#{stop_id}')")
    lines.each do |l|
      m = Modalities[l['type']] || 200
      mods << m if !mods.include?(m)
    end
    mods.to_s.gsub('[','{').gsub(']','}')
  end


  def self.stopNode(conn, layer_id, stop)
    cdkid = "gtfs.stop.#{stop['stop_id'].downcase.gsub(/\W/,'.')}"
    location = stop['location']
    stop_name = conn.escape(stop['stop_name'])

    res = conn.exec("SELECT id FROM nodes WHERE cdk_id = '#{cdkid}'")
    if res.cmd_tuples > 0
      id = res[0]['id']
      query = <<-SQL
        UPDATE nodes
        SET
          name='#{stop_name}',
          geom='#{location}'::geometry
        WHERE id=#{id}
      SQL
    else
      id = conn.exec("select nextval('nodes1_id_seq')")[0]['nextval'].to_i
      query = <<-SQL
        INSERT INTO nodes (
          id,
          cdk_id,
          layer_id,
          node_type,
          name,
          geom
        ) VALUES (
          #{id},
          '#{cdkid}',
          #{layer_id},
          2,
          '#{stop_name}',
          '#{location}'
        );
      SQL
    end

    conn.exec(query)
    return id
  end

  @@agency_names = {}

  def self.get_agency_name(conn, id)
    return @@agency_names[id] if(@@agency_names[id])
    res = conn.exec("select agency_name from gtfs.agency where agency_id = '#{id}'")
    if ( res.cmd_tuples > 0)
      @@agency_names[id] = res[0]['agency_name']
      return @@agency_names[id]
    end
    id
  end

  def self.routeNode(conn, route, members, line, dir, prefix)
    aname = route['agency_id']
    if aname && aname == ''
      aname = $prefix
    end
    cdkid = "gtfs.line.#{aname.gsub(/\W/,'')}.#{route['route_short_name'].gsub(/\W/,'')}-#{dir}".downcase
    line = 'ARRAY[' + line.join(',') + "]"
    members = "{" + members.join(',') + "}"

    name = conn.escape("#{aname} #{RevMods[route['route_type']]} #{route['route_short_name']}")

    res = conn.exec("select id from nodes where cdk_id = '#{cdkid}'")
    if( res.cmd_tuples > 0)
      id = res[0]['id']
      # update
      query = "update nodes set name='#{name}', geom=ST_MakeLine(#{line}), members='#{members}' where id=#{id}"
    else
      # insert
      id = conn.exec("select nextval('nodes1_id_seq')")[0]['nextval'].to_i
      query = <<-SQL
        INSERT INTO nodes (
          id,
          name,
          cdk_id,
          layer_id,
          node_type,
          members,
          geom
        ) VALUES (
          #{id},
          '#{name}',
          '#{cdkid}',
          #{layer_id},
          3,
          '#{members}',
          ST_MakeLine(#{line})
        );
      SQL
    end

    conn.exec(query)
    id
  end

  def self.stopNodeData(conn, layer_id, node_id, gtfsStop)
    # don't need to keep this..
    gtfsStop.delete('location')

    res = conn.exec <<-SQL
      SELECT id
      FROM node_data
      WHERE
        node_id = #{node_id}
        AND layer_id = #{layer_id}
    SQL

    if res.cmd_tuples > 0
      id = res[0]['id']

      query = <<-SQL
        UPDATE node_data
        SET
          data = #{gtfsStop.to_hstore(conn)},
          modalities = '#{GTFS_Import::modalitiesForStop(conn, gtfsStop)}'
        WHERE id = #{id}
      SQL

    else
      id = conn.exec("SELECT nextval('node_data_id_seq')")[0]['nextval'].to_i

      query = <<-SQL
        INSERT INTO node_data (
          id,
          node_id,
          layer_id,
          data,
          modalities
        ) VALUES (
          #{id},
          '#{node_id}',
          #{layer_id},
          #{gtfsStop.to_hstore(conn)},
          '#{GTFS_Import::modalitiesForStop(conn, gtfsStop)}'
        );
      SQL
    end

    conn.exec(query)
    id
  end

  def self.routeNodeData(conn, layer_id, node_id, route, val)
    mods = "{#{route['route_type']}}"

    res = conn.exec <<-SQL
      SELECT id
      FROM node_data
      WHERE
        node_id = #{node_id}
        AND layer_id = #{layer_id}
    SQL

    if( res.cmd_tuples > 0)
      id = res[0]['id']
      query  = "update node_data set"
      query += " data=#{route.to_hstore(conn)}"
      query += ", modalities='#{mods}'"
      query += ", validity='#{val}'" if val
      query += " where id=#{id}"
    else
      id = conn.exec("select nextval('node_data_id_seq')")[0]['nextval'].to_i
      query = <<-SQL
        INSERT INTO node_data (
          id,
          node_id,
          layer_id,
          data,
          modalities,
          validity
        )
      SQL

      if val
        query += <<-SQL
          VALUES (
            #{id},
            '#{node_id}',
            #{layer_id},
            #{route.to_hstore(conn)},
            '#{mods}',
            '#{val}'
          );
        SQL
      else
        query += <<-SQL
          VALUES (
            #{id},
            '#{node_id}',
            #{layer_id},
            #{route.to_hstore(conn)},
            '#{mods}',
            NULL
          );
        SQL
      end
    end

    conn.exec(query)
    id
  end

  def self.addOneRoute(conn, layer_id, route, dir, val, prefix)
    r = route['route_id']


    # puts "select * from stops_for_line('#{r}',#{dir})"
    stops = conn.exec("select * from stops_for_line('#{r}',#{dir})")
    # puts "done.."
    shape = conn.exec("select * from shape_for_line('#{r}')")
    shape = nil if shape.cmdtuples == 0

    if stops.cmdtuples > 1
      members = []
      line = []
      q = ''
      start_name = end_name = nil
      stops.each do |s|
        stop_id = s['stop_id']
        if stop_id.nil? || stop_id == ''
          next
        end
        nd = %{ (node_data.data @> '"stop_id"=>"#{stop_id}"') }
        begin
          q = "select * from node_data where layer_id = #{layer_id} and #{nd} limit 1"
          nd = conn.exec(q)
          if nd
            nd.each do |n|
              start_name = s['name'] if start_name.nil?
              end_name = s['name']
              members << n['node_id']
              if( shape.nil? )
                line << "'" + conn.exec("select geom from nodes where id = #{n['node_id'].to_i} limit 1" )[0]['geom'] + "'::geometry"
              end
            end
          end
        rescue Exception => e
          puts "addOneRoute: #{e.message}"
          puts q
          exit(1)
        end
      end
      if( members.length > 0)
        begin
          if(shape)
            shape.each do |s|
              g = s['geom']
              line << "'#{g}'"
            end
          end
          route['route_from'] = start_name
          route['route_to'] = end_name
          id = GTFS_Import::routeNode(route, members, line, dir, prefix)
          if(id)
            GTFS_Import::routeNodeData(conn, layer_id, id, route, val)
          end
        rescue
        end
        return
      end
    end
    $routes_rejected += 1
  end


end
