#!/usr/bin/env zsh

cd -- ${0:h}/..
exec rvm all do bundle exec ruby -I lib bin/clear.rb $@

