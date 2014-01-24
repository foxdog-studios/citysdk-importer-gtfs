#!/usr/bin/env ruby

require 'pg'

require 'citysdk/importers/gtfs/funcs.rb'
require 'citysdk/importers/gtfs/util.rb'

$DB_name = ARGV[0]
$DB_user = ARGV[1]
$DB_pass = ARGV[2]

$pg_csdk = nil

$gtfs_files = ['agency', 'feed_info', 'calendar_dates', 'stops', 'routes', 'trips', 'stop_times','shapes']

$tableParams = {
  'feed_info' => {
    :params  => [['feed_publisher_name', 'text'],['feed_publisher_url', 'text'], ['feed_lang', 'text'], ['feed_start_date', 'date'], ['feed_end_date', 'date'], ['feed_version', 'text'], ['agencies', 'text'], ['date_added', 'text']],
    :indexes => [],
  },
  'agency' => {
    :params  => [['agency_id', 'text', 'primary key'],['agency_name', 'text'], ['agency_url', 'text'], ['agency_timezone', 'text'], ['agency_lang', 'text']],
    :indexes => [],
  },
  'stops' => {
    :params  => [['stop_id', 'text', 'primary key'], ['stop_name', 'text'], ['location_type', 'smallint'], ['parent_station', "text default ''"], ['wheelchair_boarding', 'smallint'], ['platform_code', 'text'], ['location', 'geometry(point,4326)'] ],
    :indexes => ['create index stops_location on gtfs.stops using gist(location);','create index stops_stop_id on gtfs.stops(stop_id);'],
  },
  'calendar_dates' => {
    :params  => [['service_id', 'text'], ['date', 'date'], ['exception_type', 'smallint'] ],
    :indexes => ['create index calendar_dates_service_id on gtfs.calendar_dates(service_id);'],
  },
  'routes' => {
    :params  => [['route_id', 'text', 'primary key'], ['agency_id', 'text'], ['route_short_name', 'text'], ['route_long_name', 'text'],['route_type', 'smallint'] ],
    :indexes => ['create index routes_route_id on gtfs.routes(route_id);'],
  },
  'stop_times' => {
    :params  => [['trip_id', 'text'], ['arrival_time', 'text'], ['departure_time', 'text'], ['stop_id', 'text'], ['stop_sequence', 'smallint'], ['stop_headsign', 'text'], ['pickup_type', 'smallint'], ['drop_off_type', 'smallint'] ],
    :indexes => ['create index stop_times_stop_id_trip_id on gtfs.stop_times using btree(trip_id, stop_id)','create index stop_times_trip_id on gtfs.stop_times(trip_id);', "create index stop_times_stop_id on gtfs.stop_times(stop_id);", "create index stop_times_departure_time on gtfs.stop_times(departure_time);"  ],
  },
  'trips' => {
    :params  => [['route_id', 'text'], ['service_id', 'text'], ['trip_id', 'text'], ['trip_headsign', 'text'], ['direction_id', 'smallint'], ['wheelchair_accessible', 'smallint'], ['trip_bikes_allowed', 'smallint'], ['shape_id', 'text'] ],
    :indexes => ["create index trips_trip_id on gtfs.trips(trip_id); create index trips_route_id on gtfs.trips(route_id); ; create index trips_direction_id on gtfs.trips(direction_id);"],
  },
  'shapes' => {
    :params  => [['shape_id', 'text'], ['shape_pt_lat', 'text'], ['shape_pt_lon', 'text'], ['shape_pt_sequence', 'integer'] ],
    :indexes => ["create index shapes_shape_id on gtfs.shapes(shape_id);"],
  }

}



def createIndexes(table_name)
  $tableParams[table_name][:indexes].each do |idx|
    $pg_csdk.exec(idx)
  end
end

def createTable(table_name)
  params = $tableParams[table_name][:params].map do |p|
    p.join(' ')
  end.join(',')
  $pg_csdk.exec("create table gtfs.#{table_name} (#{params});")
end


begin
  $pg_csdk = PGconn.new('localhost', '5432', nil, nil, $DB_name, $DB_user, $DB_pass)

  res = $pg_csdk.exec("select id from layers where name = 'gtfs'");
  $gtfs_layerID = res[0]['id'].to_i if res.cmdtuples > 0
  if($gtfs_layerID.nil?)
    $stderr.puts "No gtfs layer found!"
    exit(-1)
  end

  $pg_csdk.transaction do
    $pg_csdk.exec("drop schema if exists gtfs cascade; create schema gtfs;")
    $pg_csdk.exec("delete from node_data where layer_id = #{$gtfs_layerID};")
    $pg_csdk.exec("delete from nodes where layer_id = #{$gtfs_layerID};")
    $gtfs_files.each do |f|
      createTable(f)
      createIndexes(f)
    end
    addUtilityFunctions()
  end
  GTFS_Import::do_log('Cleared GTFS layer..')
  $stderr.puts "\nCOMMIT"
rescue Exception => e
  puts e.message
ensure
  $pg_csdk.close
end

