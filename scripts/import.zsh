#!/usr/bin/env zsh

cd -- ${0:h}/..
exec rvm 2.1.1@citysdk do bundle exec ruby -I lib bin/import.rb $@

