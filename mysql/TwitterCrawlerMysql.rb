require 'nokogiri'
require 'open-uri'
require 'mysql2'
require 'method_profiler'
require 'ruby-progressbar'
#require 'htmlentities'
require 'pp'

URL  = "http://en.wikipedia.org"
PATH = "/wiki/List_of_American_film_actresses"

HEADERS_HASH = {"User-Agent" => "Ruby/#{RUBY_VERSION}"}

class TwitterCrawler
  def init_tables(con)
    actress_table = "CREATE TABLE `actresses` (
                         `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
                         `full_name` varchar(50) DEFAULT NULL,
                          PRIMARY KEY (`id`)
                     ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    twitter_table = "CREATE TABLE `twitters` (
                         `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
                         `url` text,
                         `actress_id` int(10) unsigned NOT NULL,
                         PRIMARY KEY (`id`)
                     ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    con.query("DROP TABLE IF EXISTS `actresses`;")
    con.query("DROP TABLE IF EXISTS `twitters`;")
    con.query(actress_table)
    con.query(twitter_table)
  end

  def update_twitters(con, actresses)
      progress = ProgressBar.create(:title => "Updating Twitters", :total => actresses.size, :format => '%t %B %p%% (%c/%C) [%a]')

      actresses.each do |actress|
        if not actress[:name].nil? and not actress[:name].empty? then
          actress[:name]  = actress[:name].gsub("'"," ")
          con.query("INSERT IGNORE INTO actresses (full_name) VALUES('#{actress[:name]}');")
          id = con.last_id
          
          if not actress[:twtrs].nil? and not actress[:twtrs].empty? then
            insert_values = actress[:twtrs].map { |link| "#{id},'#{link}'" } 
            insert_query = "INSERT INTO twitters (actress_id, url) VALUES (#{insert_values.join('),(')});"
            con.query(insert_query)
          end # done: if not actress[:twtrs].nil? ...
        end # done: if not actress[:name].nil? ...
        progress.increment
      end # done: actresses.each
      progress.finish
  end

  def crawl_twtrs(url, path)
    page = Nokogiri::HTML(open(url + path))
    hrefs = page.css('div.div-col.columns.column-width li a')[0..19]

    progress = ProgressBar.create(:title => "Parsing Wikis", :total => hrefs.size, :format => '%t %B %p%% (%c/%C) [%a]')

    actresses = hrefs.map do |actress|
      progress.increment
      name = actress[:title]
      href = actress[:href]
      wiki = if /\/wiki\// =~ href then href else '' end
      wiki_html = Nokogiri::HTML(open(url + wiki))

      twtr_links = wiki_html.css('div#mw-content-text.mw-content-ltr li a.external.text').map do |ext_link|
        ext_link_href = ext_link[:href]
        #
        # Some wiki pages contain multiple/inaccurate Twitter accounts; scraping all possible Twitter urls
        #
        if ext_link_href =~ /twitter.com\// then twtr = ext_link_href.scan(/(https?:\/\/)(www.|mobile.)?(twitter.com\/)(#!\/)?@?([a-zA-Z0-9_]*)\/?/).first[0..4].join('') rescue nil else ''  end
      end # done: twtr_links
      twtr_links = twtr_links.compact.uniq.reject! {|t| t.empty?}
      #
      # TODO: Validate twitter urls
      #

      actress = {:name => name, :twtrs => twtr_links}
    end # done: actresses
   
    progress.finish
 
    return actresses 
  end # done: crawl_twtrs
end # done: class TwitterWiki

begin
  #
  # TODO: Implement config file
  #
  config = { :host => 'localhost',
             :username => 'root',
             :password => '',
             :db => 'webcrawl' }

  #con = Mysql.new(config[:host], config[:username], config[:password], config[:db])
  #con.set_server_option Mysql::OPTION_MULTI_STATEMENTS_ON
  con = Mysql2::Client.new(:host => config[:host] , :username => config[:username], :password => config[:password],:database => config[:db], :flags => Mysql2::Client::MULTI_STATEMENTS, :reconnect => true)

  profiler = MethodProfiler.observe(TwitterCrawler)

  tc = TwitterCrawler.new

  actresses = tc.crawl_twtrs(URL, PATH)

  tc.init_tables(con)
  tc.update_twitters(con, actresses)

  puts profiler.report
rescue Mysql2::Error => e
  puts e
ensure
  con.close if con
end # done: Mysql2
