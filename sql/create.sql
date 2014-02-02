-- -----------------------------------------------------------------------------
-- - Preamble                                                                  -
-- -----------------------------------------------------------------------------

\set ECHO all
\set ON_ERROR_STOP on


-- -----------------------------------------------------------------------------
-- - Schema                                                                    -
-- -----------------------------------------------------------------------------

CREATE SCHEMA gtfs;


-- -----------------------------------------------------------------------------
-- - Tables                                                                    -
-- -----------------------------------------------------------------------------

-- CitySDK table
CREATE TABLE gtfs.feed (
    gtfs_id       TEXT PRIMARY KEY,
    uri           TEXT NOT NULL UNIQUE,
    last_imported TIMESTAMP WITH TIME ZONE
);


CREATE TABLE gtfs.feed_info (
    gtfs_id             TEXT PRIMARY KEY,

    feed_publisher_name TEXT NOT NULL,
    feed_publisher_url  TEXT NOT NULL,
    feed_timezone       TEXT NOT NULL,
    feed_lang           TEXT,
    feed_valid_from     DATE,
    feed_valid_to       DATE,
    feed_version        TEXT,

    FOREIGN KEY (gtfs_id)
        REFERENCES gtfs.feed
        ON DELETE CASCADE
);


CREATE TABLE gtfs.agency (
    gtfs_id TEXT NOT NULL,

    agency_id TEXT NOT NULL,
    agency_name TEXT NOT NULL,
    agency_url TEXT NOT NULL,
    agency_timezone TEXT NOT NULL,
    agency_lang TEXT,

    PRIMARY KEY (gtfs_id, agency_id),

    FOREIGN KEY (gtfs_id)
        REFERENCES gtfs.feed
        ON DELETE CASCADE
);


CREATE TABLE gtfs.routes (
    gtfs_id          TEXT NOT NULL,

    route_id         TEXT NOT NULL,
    agency_id        TEXT,
    route_short_name TEXT NOT NULL,
    route_long_name  TEXT NOT NULL,
    route_desc       TEXT,
    route_type       SMALLINT NOT NULL CHECK (route_type BETWEEN 0 AND 7),
    route_url        TEXT,
    route_color      TEXT,
    route_text_color TEXT,

    PRIMARY KEY (gtfs_id, route_id),

    FOREIGN KEY (gtfs_id, agency_id)
        REFERENCES gtfs.agency (gtfs_id, agency_id)
        ON DELETE CASCADE
);


CREATE TABLE gtfs.shapes (
    gtfs_id TEXT NOT NULL,

    shape_id TEXT NOT NULL,
    shape_pt_lat TEXT NOT NULL,
    shape_pt_lon TEXT NOT NULL,
    shape_pt_sequence INTEGER NOT NULL,
    shape_dist_traveled NUMERIC,

    PRIMARY KEY (gtfs_id, shape_id)
);


CREATE TABLE gtfs.stops (
    gtfs_id TEXT NOT NULL,

    stop_id TEXT NOT NULL,
    stop_code TEXT,
    stop_name TEXT NOT NULL,
    stop_desc TEXT,
    stop_lat DOUBLE PRECISION NOT NULL,
    stop_lon DOUBLE PRECISION NOT NULL,
    zone_id TEXT,
    stop_url TEXT,
    location_type SMALLINT CHECK (location_type IN (0, 1)),
    parent_station TEXT,

    PRIMARY KEY (gtfs_id, stop_id),

    FOREIGN KEY (gtfs_id, parent_station)
        REFERENCES gtfs.stops (gtfs_id, stop_id)
        ON DELETE CASCADE,

    UNIQUE (gtfs_id, stop_id)
);

CREATE TABLE gtfs.trips (
    gtfs_id               TEXT NOT NULL,

    route_id              TEXT NOT NULL,
    service_id            TEXT NOT NULL,
    trip_id               TEXT NOT NULL,
    trip_headsign         TEXT,
    direction_id          SMALLINT,
    wheelchair_accessible SMALLINT,
    trip_bikes_allowed    SMALLINT,
    block_id              TEXT,
    shape_id              TEXT,

    PRIMARY KEY (gtfs_id, trip_id),

    FOREIGN KEY (gtfs_id, route_id)
        REFERENCES gtfs.routes (gtfs_id, route_id)
        ON DELETE CASCADE,

    FOREIGN KEY (gtfs_id, shape_id)
        REFERENCES gtfs.shapes (gtfs_id, shape_id)
        ON DELETE CASCADE
);

CREATE TABLE gtfs.calendar_dates (
    gtfs_id        TEXT NOT NULL,

    service_id     TEXT NOT NULL,
    date           DATE NOT NULL,
    exception_type SMALLINT NOT NULL CHECK (exception_type IN (1, 2)),

    PRIMARY KEY (gtfs_id, service_id, date)
);

CREATE TABLE gtfs.stop_times (
    gtfs_id TEXT NOT NULL,

    trip_id TEXT NOT NULL,
    arrival_time TEXT NOT NULL,
    departure_time TEXT NOT NULL,
    stop_id TEXT NOT NULL,
    stop_sequence SMALLINT NOT NULL CHECK (stop_sequence >= 0),
    stop_headsign TEXT,
    pickup_type SMALLINT,
    drop_off_type SMALLINT,
    shape_dist_traveled NUMERIC CHECK (shape_dist_traveled >= 0),

    PRIMARY KEY (gtfs_id, trip_id, stop_id, stop_sequence),

    FOREIGN KEY (gtfs_id, trip_id)
        REFERENCES gtfs.trips (gtfs_id, trip_id)
        ON DELETE CASCADE,

    FOREIGN KEY (gtfs_id, stop_id)
        REFERENCES gtfs.stops (gtfs_id, stop_id)
        ON DELETE CASCADE
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
                    n
                ))
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

CREATE OR REPLACE FUNCTION gtfs.lines_for_stop(stop TEXT)
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


-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION gtfs.stops_for_line(TEXT, INTEGER)
    RETURNS TABLE(
        name TEXT,
        location TEXT,
        stop_id TEXT,
        stop_seq SMALLINT
    )
AS $$
    SELECT DISTINCT
        stops.stop_name,
        ST_AsEWKT(ST_SetSRID(ST_Point(stop_lat, stop_lon), 4326)) AS location,
        stops.stop_id,
        stop_sequence
    FROM gtfs.stop_times
    LEFT JOIN gtfs.stops ON stops.stop_id = stop_times.stop_id
    WHERE trip_id = (
        SELECT longest_trip_for_route($1, $2)
    )
    ORDER BY stop_sequence;
$$ LANGUAGE SQL;

-- vi: filetype=pgsql
