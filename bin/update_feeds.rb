require 'date'
require 'net/http'
require 'tempfile'

def get_last_modified(uri)
  http = Net::HTTP.new(uri.host, uri.port)
  response = http.head(uri.request_uri)
  last_modified = response['Last-Modified']
  Date.parse(last_modified[/.*,\s+(.*)\s+\d\d:/, 1])
end

def import_gtfs_archive(uri, prefix)
  use_ssl = uri.scheme == 'https'
  Net::HTTP.start(uri.host, uri.port, :use_ssl => use_ssl) do |http|
    request = Net::HTTP::Get.new(uri)
    http.request(request) do |response|
      Tempfile.create([prefix, '.zip']) do |archive|
        archive.binmode
        response.read_body do |chunk|
          archive.write(chunk)
        end
      end
    end
  end
end

def import_gtfs_archive_if_newer(uri, prefix, last_imported)
  last_modified = get_last_modified(uri)
  if last_modified > last_imported
    import_gtfs_archive(uri, prefix)
  end
end

def main(argv)
  uri = URI.parse(argv[0])
  prefix = argv[1]
  last_imported = Date.parse(argv[2])
  import_gtfs_archive_if_newer(uri, prefix, last_imported)
  return 0
end

if  __FILE__ == $0
  exit main(ARGV)
end

