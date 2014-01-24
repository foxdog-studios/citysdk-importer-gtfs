#!/usr/bin/env zsh

cd -- ${0:h}/..
exec rvm 1.9.3 do bundle exec ruby -I lib bin/update_feeds.rb $@

