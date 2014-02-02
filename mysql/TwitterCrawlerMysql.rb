require 'nokogiri'
require 'open-uri'
require 'parallel'
require 'benchmark'
require 'mysql'
require 'htmlentities'
require 'pp'

URL = "http://en.wikipedia.org"
LIST_URL = "/wiki/List_of_American_film_actresses"

HEADERS_HASH = {"User-Agent" => "Ruby/#{RUBY_VERSION}"}

def crawl_twtrs(url)
  begin
    con = Mysql.new('localhost','root','','webcrawl')
    con.set_server_option Mysql::OPTION_MULTI_STATEMENTS_ON
    
begin
    page = Nokogiri::HTML(open(url))

    actresses = page.css('div.div-col.columns.column-width li a').map do |actress|
      name = actress[:title]
      href = actress[:href]
      wiki = if /\/wiki\// =~ href then href else '' end
      wiki_html = Nokogiri::HTML(open(URL + wiki))

      twtr_links = wiki_html.css('div#mw-content-text.mw-content-ltr li a.external.text').map do |ext_link|
        ext_link_href = ext_link[:href]
        #
        # Some wiki pages contain multiple/inaccurate Twitter accounts; scraping all possible Twitter urls
        #
        #if ext_link_href =~ /twitter.com\/[a-zA-Z0-9_]*$/ then twtr = ext_link_href end
        if ext_link_href =~ /twitter.com\// then twtr = ext_link_href.scan(/(https?:\/\/)(www.|mobile.)?(twitter.com\/)(#!\/)?@?([a-zA-Z0-9_]*)\/?/).first[0..4].join('') rescue nil else '' end
      end # done: twtr_links
      twtr_links = twtr_links.compact.uniq.reject! {|t| t.empty?}
      con.query("INSERT IGNORE INTO actresses (full_name) VALUES ('#{name.gsub("'"," ")}');")
      rs = con.query("SELECT id FROM actresses WHERE full_name = '#{name.gsub("'"," ")}';")

      actress_id = rs.fetch_row rescue nil
      if not actress_id.nil? then
        if not twtr_links.nil? then
          twtr_links.each do |link|
            con.query("INSERT INTO twitter_accounts (actress_id, twitter_url) VALUES (#{actress_id.first}, '#{link}');")
          end
        end
      end
   end
end
  rescue Mysql::Error => e
    puts e
  ensure
    con.close if con
  end # done: Mysql
end # done: crawl_twtrs

Benchmark.bm do |bm|
  bm.report do
    crawl_twtrs(URL + LIST_URL)
  end
end

