-- -----------------------------------------------------------------------------
-- - Schema                                                                    -
-- -----------------------------------------------------------------------------

CREATE SCHEMA gtfs;


-- -----------------------------------------------------------------------------
-- - Tables                                                                    -
-- -----------------------------------------------------------------------------

CREATE TABLE gtfs.agency (
    agency_id             TEXT      PRIMARY KEY,
    agency_name           TEXT,
    agency_url            TEXT,
    agency_timezone       TEXT,
    agency_lang           TEXT
);

CREATE TABLE gtfs.calendar_dates (
    service_id            TEXT,
    date                  DATE,
    exception_type        SMALLINT
);

CREATE TABLE gtfs.feed_info (
    feed_publisher_name   TEXT,
    feed_publisher_url    TEXT,
    feed_lang             TEXT,
    feed_start_date       DATE,
    feed_end_date         DATE,
    feed_version          TEXT,
    agencies              TEXT,
    date_added            TEXT
);

CREATE TABLE gtfs.routes (
    route_id              TEXT      PRIMARY KEY,
    agency_id             TEXT,
    route_short_name      TEXT,
    route_long_name       TEXT,
    route_type            SMALLINT
);

CREATE TABLE gtfs.shapes (
    shape_id              TEXT,
    shape_pt_lat          TEXT,
    shape_pt_lon          TEXT,
    shape_pt_sequence     INTEGER
);

CREATE TABLE gtfs.stops (
    stop_id               TEXT      PRIMARY KEY,
    stop_name             TEXT,
    location_type         SMALLINT,
    parent_station        TEXT      DEFAULT '',
    wheelchair_boarding   SMALLINT,
    platform_code         TEXT,

    -- Can this be upper case? What SRID this?
    location              geometry(point, 4326)
);

CREATE TABLE gtfs.stop_times (
    trip_id               TEXT,
    arrival_time          TEXT,
    departure_time        TEXT,
    stop_id               TEXT,
    stop_sequence         SMALLINT,
    stop_headsign         TEXT,
    pickup_type           SMALLINT,
    drop_off_type         SMALLINT
);

CREATE TABLE gtfs.trips (
    route_id              TEXT,
    service_id            TEXT,
    trip_id               TEXT,
    trip_headsign         TEXT,
    direction_id          SMALLINT,
    wheelchair_accessible SMALLINT,
    trip_bikes_allowed    SMALLINT,
    shape_id              TEXT
);


-- -----------------------------------------------------------------------------
-- - Indexices                                                                 -
-- -----------------------------------------------------------------------------

CREATE INDEX calendar_dates_service_id  ON gtfs.calendar_dates(service_id);
CREATE INDEX routes_route_id            ON gtfs.routes(route_id);
CREATE INDEX shapes_shape_id            ON gtfs.shapes(shape_id);
CREATE INDEX stop_times_departure_time  ON gtfs.stop_times(departure_time);
CREATE INDEX stop_times_stop_id         ON gtfs.stop_times(stop_id);
CREATE INDEX stop_times_trip_id         ON gtfs.stop_times(trip_id);
CREATE INDEX stops_stop_id              ON gtfs.stops(stop_id);
CREATE INDEX trips_direction_id         ON gtfs.trips(direction_id);
CREATE INDEX trips_route_id             ON gtfs.trips(route_id);
CREATE INDEX trips_trip_id              ON gtfs.trips(trip_id);

CREATE INDEX stop_times_stop_id_trip_id ON gtfs.stop_times
        USING btree(trip_id, stop_id);

CREATE INDEX stops_location             ON gtfs.stops
        USING gist(location);


-- -----------------------------------------------------------------------------
-- - Functions                                                                 -
-- -----------------------------------------------------------------------------

-- Return type of transport given the smallint GTFS route_type code

CREATE OR REPLACE FUNCTION transport_type(ordinal SMALLINT)
  RETURNS TEXT
AS $$
  DECLARE pttypes text[];
  BEGIN
      pttypes[0] = 'Tram';
      pttypes[1] = 'Subway';
      pttypes[2] = 'Rail';
      pttypes[3] = 'Bus';
      pttypes[4] = 'Ferry';
      pttypes[5] = 'Cable car';
      pttypes[6] = 'Gondola';
      pttypes[7] = 'Funicular';
      RETURN pttypes[ordinal];
  END;
$$ LANGUAGE PLPGSQL;


-- -----------------------------------------------------------------------------

-- Return all routes that service a given stop_id
CREATE OR REPLACE FUNCTION rlines_for_stop(stop TEXT)
    RETURNS SETOF gtfs.routes
AS $$
    BEGIN
        RETURN query
            SELECT *
            FROM gtfs.routes
            WHERE routes.route_id IN (
                SELECT DISTINCT trips.route_id
                FROM gtfs.trips
                WHERE trip_id IN (
                    SELECT DISTINCT trip_id
                    FROM gtfs.stop_times
                    WHERE stop_id = stop
                )
            );
    END;
$$ LANGUAGE PLPGSQL;


-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION line_schedule(
    routeid   TEXT,
    direction INTEGER,
    day       INTEGER
)
    RETURNS TABLE(
        trip_id TEXT,
        stop_id TEXT,
        departure_time TEXT
    )
AS $$
    BEGIN
        RETURN query
            SELECT
                gtfs.stop_times.trip_id,
                gtfs.stop_times.stop_id,
                gtfs.stop_times.departure_time
            FROM gtfs.stop_times
            WHERE gtfs.stop_times.trip_id in (
                SELECT gtfs.trips.trip_id
                FROM gtfs.trips
                INNER JOIN gtfs.calendar_dates USING (service_id)
                WHERE gtfs.trips.route_id = routeid
                    AND gtfs.trips.direction_id = direction
                    AND gtfs.calendar_dates.date
                        = current_date + (day::text || ' days')::interval
            );
    END;
$$ LANGUAGE PLPGSQL;


-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stop_now(stopid TEXT, tzoffset TEXT)
    RETURNS TABLE(
        route_id text,
        direction_id smallint,
        route_type text,
        route_name text,
        headsign text,
        departure text,
        agency_id text
    )
AS $$
    DECLARE offs interval;
    DECLARE n timestamp = now() - tzoffset::interval;
    BEGIN
        RETURN query
            SELECT
                trips.route_id::text,
                trips.direction_id,
                transport_type(routes.route_type),
                routes.route_short_name::text,
                trips.trip_headsign::text,
                stop_times.departure_time::text,
                routes.agency_id::text
            FROM
                gtfs.calendar_dates,
                gtfs.trips,
                gtfs.stop_times,
                gtfs.routes
            WHERE
                stop_times.stop_id = stopid
                AND stop_times.trip_id = trips.trip_id
                AND routes.route_id = trips.route_id
                AND (departs_within(
                        stop_times.departure_time,
                        '-5 minutes'::interval,
                        n
                ))
                OR (departs_within(
                    stop_times.departure_time,
                    '55 minutes'::interval,
                    n)
                )
                AND trips.service_id = calendar_dates.service_id
                AND calendar_dates.date = now()::date
                AND calendar_dates.exception_type = 1
            ORDER BY stop_times.departure_time;
    END;
$$ LANGUAGE PLPGSQL;


-- -----------------------------------------------------------------------------

-- Return trips from the current stop for today + n days
CREATE OR REPLACE FUNCTION departs_from_stop(stopid TEXT, days INTEGER)
    RETURNS TABLE(
        route_id     TEXT,
        direction_id SMALLINT,
        route_type   TEXT,
        route_name   TEXT,
        headsign     TEXT,
        departure    TEXT,
        agency_id    TEXT
    )
AS $$
    BEGIN
        RETURN query
            SELECT
                trips.route_id::text,
                trips.direction_id,
                transport_type(routes.route_type),
                routes.route_short_name::text,
                trips.trip_headsign::text,
                stop_times.departure_time::text,
                routes.agency_id::text
            FROM
                gtfs.calendar_dates,
                gtfs.trips,
                gtfs.stop_times,
                gtfs.routes
            WHERE
                stop_times.stop_id = stopid
                AND stop_times.trip_id = trips.trip_id
                AND routes.route_id = trips.route_id
                AND trips.service_id = calendar_dates.service_id
                AND calendar_dates.date = (
                    now() + (days::text || ' days')::interval
                )::date
                AND calendar_dates.exception_type = 1
            ORDER BY route_id,stop_times.departure_time;
    END;
$$ LANGUAGE PLPGSQL;


-- -----------------------------------------------------------------------------

-- Return trips from the current stop for today.

CREATE OR REPLACE FUNCTION departs_from_stop_today(stopid TEXT)
    RETURNS table(
        route_id     TEXT,
        direction_id SMALLINT,
        route_type   TEXT,
        route_name   TEXT,
        headsign     TEXT,
        departure    TEXT,
        agency_id    TEXT
    )
AS $$
BEGIN
    RETURN query SELECT * FROM departs_from_stop(stopid, 0);
END;
$$ LANGUAGE PLPGSQL;


-- -----------------------------------------------------------------------------

-- Return trips from the current stop within the given time interval.

CREATE OR REPLACE FUNCTION departs_from_stop_within(stopid text, i interval)
    RETURNS TABLE(
        route_id text,
        direction_id smallint,
        route_type text,
        route_name text,
        headsign text,
        departure text
    )
AS $$
    DECLARE n TIMESTAMP = now();
    BEGIN
        RETURN query
            SELECT
                trips.route_id::text,
                trips.direction_id,
                transport_type(routes.route_type),
                routes.route_short_name::text,
                trips.trip_headsign::text,
                stop_times.departure_time::text
            FROM
                gtfs.calendar_dates,
                gtfs.trips,
                gtfs.stop_times,
                gtfs.routes
            WHERE
                stop_times.stop_id = stopid
                AND stop_times.trip_id = trips.trip_id
                AND routes.route_id = trips.route_id
                AND departs_within(stop_times.departure_time, i, n)
                AND trips.service_id = calendar_dates.service_id
                AND calendar_dates.date = now()::date
                AND calendar_dates.exception_type = 1
            ORDER BY stop_times.departure_time;
    END;
$$ LANGUAGE PLPGSQL;


-- -----------------------------------------------------------------------------

-- check wether a departure_time from the stop_times table is within <interval> from now

CREATE OR REPLACE FUNCTION departs_within(
    deptime TEXT,
    i       INTERVAL,
    nu      TIMESTAMP
)
    RETURNS BOOLEAN
AS $$
    DECLARE
        dparts TIMESTAMP;
        now    TIMESTAMP = now();
    BEGIN
        dparts := (nu - localtime) + deptime::interval;
        RETURN (
            dparts > now     AND dparts < now + i
        ) OR (
            dparts > now + i AND dparts < now
        );
    END;
$$ LANGUAGE PLPGSQL;


-- -----------------------------------------------------------------------------

-- Given a route, return the trip_id of the longest trip on that route

CREATE OR REPLACE FUNCTION longest_trip_for_route(
    route     TEXT,
    direction INTEGER
)
    returns TEXT
AS $$
    BEGIN
        RETURN stop_times.trip_id
        FROM gtfs.stop_times
        WHERE trip_id IN (
            SELECT trip_id
            FROM gtfs.trips
            WHERE
                route_id = route
                AND direction_id = direction
        )
        GROUP BY gtfs.stop_times.trip_id
        ORDER BY count(gtfs.stop_times.stop_id) DESC
        LIMIT 1;
    END;
$$ LANGUAGE PLPGSQL;


-- -----------------------------------------------------------------------------

-- Return all routes that service a given stop_id

CREATE OR REPLACE FUNCTION lines_for_stop(stop TEXT)
    RETURNS TABLE(
        route_id text,
        name     text,
        agency   text,
        type     text
    )
AS $$
    BEGIN
        RETURN query
        SELECT
            routes.route_id::text,
            route_short_name::text,
            agency_id::text,
            transport_type(route_type)
        FROM
            gtfs.routes
        WHERE
            routes.route_id IN (
                SELECT DISTINCT trips.route_id
                FROM gtfs.trips
                WHERE trip_id IN (
                    SELECT DISTINCT trip_id
                    FROM gtfs.stop_times
                    WHERE stop_id = stop
                )
            );
      END;
$$ LANGUAGE PLPGSQL;


-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stops_for_line(TEXT, INTEGER)
    RETURNS TABLE(
        name TEXT,
        location TEXT,
        stop_id TEXT,
        stop_seq SMALLINT
    )
AS $$
    SELECT
        DISTINCT stops.stop_name,
        st_asewkt(stops.location) AS location,
        stops.stop_id,
        stop_sequence
    FROM gtfs.stop_times
    LEFT JOIN gtfs.stops ON stops.stop_id = stop_times.stop_id
    WHERE trip_id = (
        SELECT longest_trip_for_route($1, $2)
    )
    ORDER BY stop_sequence;
$$ LANGUAGE SQL;


-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION shape_for_line(line TEXT)
    RETURNS TABLE(geom geometry, seq INTEGER)
AS $$
    DECLARE tripid TEXT;
    BEGIN
        SELECT longest_trip_for_route(line, 1) INTO tripid;

        RETURN query
            SELECT
                ST_SetSRID(
                    ST_Point(shape_pt_lon::float, shape_pt_lat::float),
                    4326
                ) AS geom,
                shape_pt_sequence AS seq
            FROM gtfs.shapes
            WHERE shape_id = (
                SELECT shape_id
                FROM gtfs.trips
                WHERE trip_id = tripid
                LIMIT 1
            )
            ORDER BY seq;
    END;
$$ LANGUAGE PLPGSQL;

-- vi: filetype=pgsql
