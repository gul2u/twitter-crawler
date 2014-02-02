require 'nokogiri'
require 'open-uri'
require 'benchmark'

URL = "http://en.wikipedia.org"
LIST_URL = "/wiki/List_of_American_film_actresses"

HEADERS_HASH = {"User-Agent" => "Ruby/#{RUBY_VERSION}"}

def crawl_twtrs(url, file)
  page = Nokogiri::HTML(open(url))

  file = File.new(file, "w+:UTF-8")
  file.puts("ACTRESS|TWITTER_URL\r\n")
  actresses = page.css('div.div-col.columns.column-width li a').map do |actress|
    name = actress[:title]
    href = actress[:href]
    wiki = if /\/wiki\// =~ href then href else '' end
    wiki_page = Nokogiri::HTML(open(URL + wiki))
    twtr_links = wiki_page.css('div#mw-content-text.mw-content-ltr li a.external.text').map do |ext_link|
      ext_link_href = ext_link[:href]
      #
      # Some wiki pages contain multiple/inaccurate Twitter accounts; scraping all possible Twitter urls
      #

      # if ext_link_href =~ /twitter.com\/[a-zA-Z0-9_]*$/ then twtr = ext_link_href end
      if ext_link_href =~ /twitter.com\// then twtr = ext_link_href.scan(/(https?:\/\/)(www.|mobile.)?(twitter.com\/)(#!\/)?@?([a-zA-Z0-9_]*)\/?/).first[0..4].join('') rescue nil else '' end
    end
    #puts twtr_links.compact.uniq.reject! {|t| t.empty?}
    file.puts [name, twtr_links.compact.uniq.reject! {|t| t.empty?}].join('|') +  "\r\n"
  end
  file.close
end

Benchmark.bm do |bm|
  bm.report do
    crawl_twtrs(URL + LIST_URL, "twtrs.txt")
  end
end
