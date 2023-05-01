package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Crawler {

    public static void run(FlameContext ctx, String[] args) throws Exception {

        // normalise seed urls
        ArrayList<String> normalised = new ArrayList<>();
        String seed = args[0];
        String normalisedUrl = normaliseLink(seed, seed);
        if (normalisedUrl != null)
            normalised.add(normalisedUrl);

        // blacklist
        String blacklistName = null;
        ArrayList<Pattern> blacklist = new ArrayList<>();
        if(args.length > 1) {
            blacklistName = args[1];

            Iterator<Row> itr = ctx.getKVS().scan(blacklistName);
            while (itr.hasNext()) {
                Row row = itr.next();
                String pattern = row.get("pattern");
                if (pattern != null) {
                    blacklist.add(Pattern.compile(pattern.replace("*", ".*") + "/.*"));
                }
            }
        }


        if (normalised.size() == 0) {
            ctx.output("No URLs provided to crawl");
            return;
        }

        ctx.output("OK");

        ctx.getKVS().persist("crawl");
        ctx.getKVS().persist("hosts");
        ctx.getKVS().persist("seen");

        FlameRDD urlQueue;
        urlQueue = ctx.parallelize(normalised);

        //add shutdown hook
        FlameRDD finalUrlQueue = urlQueue;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("Shutting down");
                ctx.getKVS().persist("urlQueue");
                finalUrlQueue.saveAsTable("urlQueue");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));

        while (urlQueue.count() > 0) {
            urlQueue = urlQueue.flatMap(url -> {
                try {
                    // check we haven't already crawled this url
                    KVSClient kvs = ctx.getKVS();
                    if (kvs.existsRow("crawl", Hasher.hash(url))) {
                        return List.of();
                    }

                    // check last time we accessed this host
                    URI uri = new URI(url);
                    String hostHash = Hasher.hash(uri.getHost());
                    if (hostHash == null) {
                        return List.of();
                    }

                    // dont accept .jpg, .jpeg, .gif, .png, or .txt.
                    if (url.endsWith(".jpg") || url.endsWith(".jpeg") || url.endsWith(".gif")
                            || url.endsWith(".png") || url.endsWith(".txt")) {
                        return List.of();
                    }

                    // only accept urls with http or https
                    if (!url.startsWith("http://") && !url.startsWith("https://")) {
                        return List.of();
                    }


                    if (kvs.existsRow("hosts", hostHash)) {
                        Row hostRow = kvs.getRow("hosts", hostHash);
                        String lastAccessString = hostRow.get("lastAccessed");
                        if (lastAccessString == null) {
                            hostRow.put("lastAccessed", String.valueOf(System.currentTimeMillis()));
                        } else {
                            long lastAccess = Long.parseLong(lastAccessString);
                            if (System.currentTimeMillis() - lastAccess < 500) {
                                // we accessed recently, so don't crawl
                                return List.of(url);
                            }
                        }
                    } else {
                        // get robots.txt
                        parseRobots(url, kvs);
                    }

                    // check if is in blacklist
                    for (Pattern pattern : blacklist) {
                        Matcher matcher = pattern.matcher(url);
                        if (matcher.matches()) {
                            return List.of();
                        }
                    }

                    // check if url is allowed by robots.txt
                    if (!isAllowed(kvs, url)) {
                        return List.of();
                    }

                    System.out.println("Crawling " + url);

                    // establish connection + headers
                    HttpURLConnection connHEAD = (HttpURLConnection) new URL(url).openConnection();
                    connHEAD.setRequestProperty("User-Agent", "cis5550-crawler");
                    connHEAD.setConnectTimeout(1000);
                    connHEAD.setReadTimeout(1000);
                    connHEAD.setInstanceFollowRedirects(false);
                    // HEAD request
                    connHEAD.setRequestMethod("HEAD");
                    connHEAD.connect();

                    // handle redirects codes  (301, 302, 303, 307, 308)
                    int status = connHEAD.getResponseCode();
                    connHEAD.disconnect();
                    if (status == 301 || status == 302 || status == 303 || status == 307 || status == 308) {
                        // save status code under current
                        Row row = new Row(Hasher.hash(url));
                        row.put("url", url);
                        row.put("responseCode", String.valueOf(status));
                        kvs.putRow("crawl", row);

                        String redirectURL = connHEAD.getHeaderField("Location");
                        String redirectNormalised = normaliseLink(url, redirectURL);
                        assert redirectNormalised != null;
                        return List.of(redirectNormalised);
                    }

                    if (connHEAD.getResponseCode() != 200) {
                        Row row = new Row(Hasher.hash(url));
                        row.put("url", url);
                        row.put("page", connHEAD.getResponseMessage());
                        row.put("responseCode", String.valueOf(status));
                        kvs.putRow("crawl", row);
                        return List.of();
                    }

                    // check content type
                    String contentType = connHEAD.getHeaderField("Content-Type");
                    if (contentType == null || !contentType.startsWith("text/html")) {
                        Row row = new Row(Hasher.hash(url));
                        row.put("url", url);
                        row.put("page", connHEAD.getResponseMessage());
                        row.put("responseCode", String.valueOf(status));
                        if (contentType != null) {
                            row.put("contentType", contentType);
                        }

                        String contentLength = connHEAD.getHeaderField("Content-Length");
                        if (contentLength != null) {
                            row.put("length", contentLength);
                        }

                        kvs.putRow("crawl", row);
                        return List.of();
                    }


                    // GET request
                    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
                    conn.setRequestProperty("User-Agent", "cis5550-crawler");
                    conn.setConnectTimeout(1000);
                    conn.setReadTimeout(1000);
                    conn.setRequestMethod("GET");
                    conn.setInstanceFollowRedirects(false);
                    conn.connect();

                    if (conn.getResponseCode() != 200) {
                        return List.of();
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
                        System.out.println("Content seen: " + url + " is the same as " + canonicalURL);
                        kvs.putRow("crawl", row);
                        return List.of();
                    }

                    // add to "crawl" table
                    Row row = new Row(Hasher.hash(url));
                    row.put("url", url);
                    row.put("page", contentBytes);
                    row.put("responseCode", "200");
                    row.put("contentType", "text/html");

                    String contentLength = conn.getHeaderField("Content-Length");
                    System.out.println("Content length: " + contentLength);
                    if (contentLength != null) {
                        row.put("length", contentLength);
                    }
                    kvs.putRow("crawl", row);

                    // add to seen table
                    Row seenRow = new Row(pageHash);
                    seenRow.put("url", url);
                    kvs.putRow("seen", seenRow);

                    // extract links
                    List<String> links = extractLinks(new String(contentBytes), url);
                    System.out.println("Found " + links.size() + " links in " + url + ":");
                    for (String link : links) {
                        System.out.println(link);
                    }
                    return links;
                } catch (Exception e) {
                    System.out.println("Error crawling " + url);
                    e.printStackTrace();
                    return List.of();
                }
            });

        }
    }

    public static List<String> extractLinks(String html, String base) {
        HashSet<String> links = new HashSet<>();
        Pattern pattern = Pattern.compile("(?i)<a\\s+(?:[^>]*?\\s+)?href=([\"'])(.*?)\\1");
        Matcher matcher = pattern.matcher(html);
        while (matcher.find()) {
            String link = matcher.group(2).trim();
            // normalise link
            link = normaliseLink(base, link);
            if (link != null && !link.isBlank() && !link.startsWith("#")) {
                links.add(link);
            }
        }
        return links.stream().toList();
    }


    private static String normaliseLink(String base, String link) {
        try {
            URI baseURI = new URI(base).normalize();
            URI linkURI = new URI(link).normalize();


            // If resulting URL is empty, discard it
            if (linkURI.toString().isBlank()) {
                return null;
            }

            // If the URL is relative, resolve it against the base URL
            if (!linkURI.isAbsolute()) {
                linkURI = baseURI.resolve(linkURI);
            }


            // If resulting URL does not have a host part, prepend the host part from the base URL
            if (linkURI.getHost() == null) {
                linkURI = new URI(baseURI.getScheme(), baseURI.getUserInfo(), baseURI.getHost(), baseURI.getPort(),
                        linkURI.getPath(), null, null);
            }

            // If resulting URL has a host part but no port number, add the default port number for the protocol
            if (linkURI.getPort() == -1 && linkURI.getScheme().equals("http")) {
                linkURI = new URI(linkURI.getScheme(), linkURI.getUserInfo(), linkURI.getHost(), 80,
                        linkURI.getPath(), null, null);
            } else if (linkURI.getPort() == -1 && linkURI.getScheme().equals("https")) {
                linkURI = new URI(linkURI.getScheme(), linkURI.getUserInfo(), linkURI.getHost(), 443,
                        linkURI.getPath(), null, null);
            }

            String path = linkURI.getPath().isBlank() ? "/" : linkURI.getPath();
            path = path.replaceAll("(&#..)", "/");
            //remove fragment
            linkURI = new URI(linkURI.getScheme(), linkURI.getUserInfo(), linkURI.getHost(), linkURI.getPort(),
                    path, null, null);

            return linkURI.toString();

        } catch (URISyntaxException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static void parseRobots(String url, KVSClient kvs) throws IOException {
        // read robots txt
        String userAgent = "cis5550-crawler";

        List<String> allowedUrls = new ArrayList<>();
        List<String> disallowedUrls = new ArrayList<>();
        URI uri;
        try {
            uri = new URI(url).resolve("/robots.txt");
        } catch (URISyntaxException e) {
            e.printStackTrace();
            return;
        }


        InputStream stream = null;
        try {
            stream = uri.toURL().openStream();
        } catch (IOException e) {
            // no robots.txt
            Row row = new Row(Hasher.hash(uri.getHost()));
            row.put("allowed", allowedUrls.toString());
            row.put("disallowed", disallowedUrls.toString());
            kvs.putRow("hosts", row);
            System.out.println("No robots.txt for " + uri.getHost());
            return;
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        String line;
        boolean userAgentMatched = false;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("User-agent:")) {
                String agent = line.substring(11).trim();
                userAgentMatched = userAgent.equals(agent) || agent.equals("*");
            } else if (line.startsWith("Allow:") && userAgentMatched) {
                String urlx = line.substring(6).trim();
                allowedUrls.add(uri.resolve(urlx).toString());
            } else if (line.startsWith("Disallow:") && userAgentMatched) {
                String urlx = line.substring(9).trim();
                disallowedUrls.add(uri.resolve(urlx).toString());
            }
        }

        // add to "hosts" table
        Row row = new Row(Hasher.hash(uri.getHost()));
        row.put("allowed", allowedUrls.toString());
        row.put("disallowed", disallowedUrls.toString());
        kvs.putRow("hosts", row);
    }

    private static boolean isAllowed(KVSClient kvs, String url) throws URISyntaxException, IOException {
        String host = Hasher.hash(new URI(url).getHost());
        String allowedString = kvs.getRow("hosts", host).get("allowed");
        String disallowedString = kvs.getRow("hosts", host).get("disallowed");
        String[] allowedUrls = allowedString.substring(1, allowedString.length() - 1)
                                                  .split(", ");
        String[] disallowedUrls = disallowedString.substring(1, disallowedString.length() - 1)
                                                  .split(", ");


        for (String disallowedUrl : disallowedUrls) {
            if (Pattern.matches(disallowedUrl.replace("*", ".*") + "/.*", url)) {
                return false;
            }
        }
        for (String allowedUrl : allowedUrls) {
            if (Pattern.matches(allowedUrl.replace("*", ".*") + "/.*", url)) {
                return true;
            }
        }

        return true;
    }

}
