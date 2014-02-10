DROP SCHEMA IF EXISTS gtfs CASCADE;

SELECT id FROM layers WHERE name = 'gtfs';
DELETE FROM node_data WHERE layer_id = id;
DELETE FROM nodes WHERE layer_id = id;

