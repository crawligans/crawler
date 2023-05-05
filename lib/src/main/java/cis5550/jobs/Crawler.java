package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameContextImpl;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Crawler {

    public static final String LAST_ACCESSED_TABLE = "hosts";
    public static final String LAST_ACCESSED_KEY = "lastAccessed";
    public static final String ROBOTSTXT_KEY = "robotstxt";
    private static final String AGENT_STRING = "cis5550-crawler";
    private static final Logger logger = Logger.getLogger(Crawler.class);

    public static void run(FlameContext ctx0, String[] args) throws Exception {
        FlameRDD urlQueue;
        FlameContextImpl ctx = (FlameContextImpl) ctx0;

        Iterator<Row> savedQueue = ctx.getKVS().scan("urlQueue");
        //add all in "urlQueue" table to normalised
        Stream<String> normalised = StreamSupport.stream(
            ((Iterable<Row>) () -> savedQueue).spliterator(), true).map(r -> r.get("value"));
        if (args.length > 0) {
            normalised = Stream.concat(normalised, Stream.of(args[0]));
        }

        normalised = normalised.map(spec -> {
            try {
                return normalizeUrl(new URL(spec));
            } catch (MalformedURLException e) {
                return null;
            }
        }).filter(Objects::nonNull).map(URL::toExternalForm);

        // blacklist
        Iterator<Row> blacklistPatterns = ctx.getKVS().scan(args[1]);
        List<Pattern> blacklist = StreamSupport.stream(
                ((Iterable<Row>) () -> blacklistPatterns).spliterator(), true)
            .map(row -> row.get("pattern")).filter(Objects::nonNull).distinct()
            .map(pattern -> pattern.replaceAll("\\*", ".*") + "/.*").map(Pattern::compile).toList();

        ctx.getKVS().persist("crawl");
        ctx.getKVS().persist("hosts");
        ctx.getKVS().persist("seen");

        urlQueue = ctx.parallelize(normalised);
        while (urlQueue.count() > 0) {
            try {
                FlameRDD newUrlQueue = urlQueue.flatMap(url -> {
                    try {
                        // check we haven't already crawled this url
                        KVSClient kvs = ctx.getKVS();
                        if (kvs.existsRow("crawl", Hasher.hash(url))) {
                            return Collections::emptyIterator;
                        }

                        // check last time we accessed this host
                        URL uri = new URL(url);
                        String hostHash = Hasher.hash(uri.getHost());
                        if (hostHash == null) {
                            return Collections::emptyIterator;
                        }

                        // dont accept .jpg, .jpeg, .gif, .png, or .txt.
                        if (Stream.of(".jpg", ".jpeg", ".gif", ".png", ".txt")
                            .anyMatch(url::endsWith)) {
                            return Collections::emptyIterator;
                        }

                        // only accept urls with http or https
                        if (Stream.of("http", "https")
                            .noneMatch(uri.getProtocol().toLowerCase()::equals)) {
                            return Collections::emptyIterator;
                        }

                        // check if is in blacklist
                        if (blacklist.stream().map(pattern -> pattern.matcher(url))
                            .anyMatch(Matcher::matches)) {
                            return Collections::emptyIterator;
                        }

                        // check if url is allowed by robots.txt
                        URL hostRoot = new URL(uri, "/");
                        Optional<Row> hostInfo = Optional.ofNullable(
                            kvs.getRow(LAST_ACCESSED_TABLE, hostRoot.toString()));
                        String robotsTxt = hostInfo.map(r -> r.get(ROBOTSTXT_KEY)).orElseGet(() -> {
                            try {
                                URL robotsTxtUrl = new URL(uri, "/robots.txt");
                                HttpURLConnection robotsTxtConnection = (HttpURLConnection) robotsTxtUrl.openConnection();
                                robotsTxtConnection.setRequestMethod("GET");
                                robotsTxtConnection.setRequestProperty("User-Agent", AGENT_STRING);
                                robotsTxtConnection.setConnectTimeout(500);
                                robotsTxtConnection.setReadTimeout(1000);
                                robotsTxtConnection.connect();
                                String robotsTxt_ = null;
                                if (robotsTxtConnection.getResponseCode() == 200) {
                                    robotsTxt_ = new String(
                                        robotsTxtConnection.getInputStream().readAllBytes(),
                                        Objects.requireNonNullElse(
                                            robotsTxtConnection.getContentEncoding(),
                                            StandardCharsets.UTF_8.toString()));
                                    Row hostRow = new Row(hostRoot.toString());
                                    hostRow.put(LAST_ACCESSED_KEY,
                                        String.valueOf(Instant.now().getEpochSecond()));
                                    hostRow.put(ROBOTSTXT_KEY, robotsTxt_);
                                    kvs.putRow(LAST_ACCESSED_TABLE, hostRow);
                                }
                                robotsTxtConnection.disconnect();
                                return robotsTxt_;
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
                        if (robotsTxt != null) {
                            String[] urlSegments = URLParser.parseURL(url);
                            List<Entry<List<String>, List<Entry<String, String>>>> robotsTxtRecords = parseRobotsTxt(
                                robotsTxt);
                            List<Map.Entry<String, String>> rules = findFirstRobotsTxtRecord(
                                robotsTxtRecords);
                            if (!allows(rules, urlSegments[3])) {
                                return Collections::emptyIterator;
                            }
                            double minInterval = rules.stream()
                                .filter(r -> r.getKey().equalsIgnoreCase("Crawl-delay"))
                                .map(Entry::getValue).findFirst().map(Double::parseDouble)
                                .orElse(0.0);
                            long lastAccessed = hostInfo.map(r -> r.get(LAST_ACCESSED_KEY))
                                .map(s -> {
                                    try {
                                        return Long.parseUnsignedLong(s);
                                    } catch (NumberFormatException e) {
                                        return null;
                                    }
                                }).orElse(0L);
                            long now = Instant.now().getEpochSecond();
                            if (now - lastAccessed <= Math.ceil(minInterval)) {
                                logger.warn("Crawl Delay not reached: " + url);
                                return Collections.singleton(url);
                            }
                        }

                        logger.info("Crawling " + url);

                        // establish connection + headers
                        HttpURLConnection connHEAD = (HttpURLConnection) new URL(
                            url).openConnection();
                        connHEAD.setRequestProperty("User-Agent", AGENT_STRING);
                        connHEAD.setConnectTimeout(500);
                        connHEAD.setReadTimeout(1000);
                        connHEAD.setInstanceFollowRedirects(false);
                        // HEAD request
                        connHEAD.setRequestMethod("HEAD");
                        connHEAD.connect();

                        // handle redirects codes  (301, 302, 303, 307, 308)
                        int status = connHEAD.getResponseCode();
                        String contentType = connHEAD.getContentType();
                        int contentLength = connHEAD.getContentLength();

                        connHEAD.disconnect();
                        if (List.of(301, 302, 303, 307, 308).contains(status)) {
                            // save status code under current
                            Row row = new Row(Hasher.hash(url));
                            row.put("url", url);
                            row.put("responseCode", String.valueOf(status));
                            kvs.putRow("crawl", row);

                            String redirectURL = connHEAD.getHeaderField("Location");
                            String redirectNormalised = normaliseLink(url, redirectURL);
                            assert redirectNormalised != null;
                            kvs.put("urlQueue", Hasher.hash(redirectNormalised), "url",
                                redirectNormalised.getBytes());
                            return List.of(redirectNormalised);
                        }

                        if (connHEAD.getResponseCode() != 200 || !"text/html".equalsIgnoreCase(
                            contentType)) {
                            Row row = new Row(Hasher.hash(url));
                            row.put("page", connHEAD.getResponseMessage());
                            row.put("responseCode", String.valueOf(status));
                            kvs.putRow("crawl", row);
                            return Collections::emptyIterator;
                        }

                        if (!contentType.startsWith("text/html")) {
                            Row row = new Row(Hasher.hash(url));
                            row.put("url", url);
                            row.put("page", connHEAD.getResponseMessage());
                            row.put("responseCode", String.valueOf(status));
                            row.put("contentType", contentType);
                            row.put("length", String.valueOf(contentLength));
                            kvs.putRow("crawl", row);
                            return List.of();
                        }

                        // GET request
                        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
                        conn.setRequestProperty("User-Agent", AGENT_STRING);
                        conn.setConnectTimeout(1000);
                        conn.setReadTimeout(1000);
                        conn.setRequestMethod("GET");
                        conn.setInstanceFollowRedirects(false);
                        conn.connect();

                        if (conn.getResponseCode() != 200) {
                            return Collections::emptyIterator;
                        }

                        byte[] contentBytes = conn.getInputStream().readAllBytes();
                        conn.disconnect();

                        // content-seen test
                        String pageHash = Hasher.hash(Arrays.toString(contentBytes));
                        if (kvs.existsRow("seen", pageHash)) {
                            Row row = new Row(Hasher.hash(url));
                            row.put("url", url);
                            String canonicalURL = kvs.getRow("seen", pageHash).get("url");
                            row.put("canonicalURL", canonicalURL);
                            row.put("responseCode", "200");
                            row.put("contentType", "text/html");
                            logger.warn("Content seen: " + url + " is the same as " + canonicalURL);
                            kvs.putRow("crawl", row);
                            return Collections::emptyIterator;
                        }

                        // add to "crawl" table
                        Row row = new Row(Hasher.hash(url));
                        row.put("url", url);
                        row.put("page", contentBytes);
                        row.put("responseCode", "200");
                        row.put("contentType", "text/html");

                        logger.info("Content length: " + contentLength);
                        row.put("length", String.valueOf(contentLength));
                        kvs.putRow("crawl", row);

                        // add to seen table
                        kvs.put("seen", pageHash, "url", url);

                        // extract links
                        List<String> links = extractLinks(new String(contentBytes), url);
                        logger.debug("Found " + links.size() + " links in " + url + ":");
                        links.forEach(logger::debug);
                        return links;
                    } catch (Exception e) {
                        logger.error("Error crawling " + url, e);
                        return Collections::emptyIterator;
                    }
                });
                urlQueue.drop();
                urlQueue = newUrlQueue;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                ctx.getKVS().persist("urlQueue");
                urlQueue.flatMap(url -> {
                    ctx.getKVS().put("urlQueue", Hasher.hash(url), "url", url);
                    return Collections::emptyIterator;
                });
            }

            // rate limit
            Thread.sleep(500);
        }
    }

    public static List<String> extractLinks(String html, String base) throws MalformedURLException {
        Pattern pattern = Pattern.compile("(?i)<(a|img)\\s+(?:[^>]*?\\s+)?href=([\"'])(.*?)\\1");
        return pattern.matcher(html).results().map(m -> m.group(2)).filter(Objects::nonNull)
            .distinct().toList();
    }

    public static URL normalizeUrl(URL url) throws MalformedURLException {
        String protocol = url.getProtocol();
        String host = url.getHost();
        int port = url.getPort() < 0 ? url.getDefaultPort() : url.getPort();
        String file = url.getPath();
        return new URL(protocol, host, port, "".equals(file) ? "/" : file);
    }

    private static String normaliseLink(String base, String link) throws MalformedURLException {
        return normalizeUrl(new URL(new URL(base), link)).toExternalForm();
    }

    private static List<Map.Entry<List<String>, List<Map.Entry<String, String>>>> parseRobotsTxt(
        String robotsTxt) {
        return Arrays.stream(robotsTxt.split("\n(\\s*(#[^\n]*)?\n)+"))
            .map(record -> record.split("\n")).map(Arrays::stream).map(record -> {
                Iterable<Entry<String, String>> ruleLines = () -> record.map(
                        line -> line.split(":", 2))
                    .map(r -> Map.entry(r[0].strip(), r.length > 1 ? r[1].strip() : "")).iterator();
                List<String> userAgents = new ArrayList<>();
                List<Map.Entry<String, String>> rules = new ArrayList<>();
                for (Map.Entry<String, String> ruleLine : ruleLines) {
                    if (ruleLine.getKey().equalsIgnoreCase("User-agent")) {
                        userAgents.add(ruleLine.getValue());
                    } else {
                        rules.add(ruleLine);
                    }
                }
                return Map.entry(userAgents, rules);
            }).toList();
    }

    private static List<Map.Entry<String, String>> findFirstRobotsTxtRecord(
        List<Entry<List<String>, List<Entry<String, String>>>> robotsTxtRecords) {
        return robotsTxtRecords.stream().filter(r -> r.getKey().stream()
                .anyMatch(agent -> agent.toLowerCase().contains(Crawler.AGENT_STRING))).findFirst()
            .map(Entry::getValue).orElseGet(() -> robotsTxtRecords.stream().filter(
                    r -> r.getKey().stream().anyMatch(agent -> Crawler.AGENT_STRING.matches(
                        agent.strip().replaceAll("\\*", ".*").replaceAll("\\?", ".")))).findFirst()
                .map(Entry::getValue).orElseGet(() -> List.of(Map.entry("Allow", "/"))));
    }

    private static boolean allows(List<Map.Entry<String, String>> rules, String route) {
        for (Map.Entry<String, String> rule : rules) {
            if (route.startsWith(rule.getValue())) {
                if (rule.getKey().equalsIgnoreCase("Allow")) {
                    return true;
                } else if (rule.getKey().equalsIgnoreCase("Disallow")) {
                    return rule.getValue().equalsIgnoreCase("");
                }
            }
        }
        return true;
    }

}
