package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;


/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final Duration timeout;
  private final int popularWordCount;
  private final int maxDepth;
  private final List<Pattern> ignoredUrls;
  private final ForkJoinPool pool;
  private final PageParserFactory parserFactory;

  @Inject
  ParallelWebCrawler(
      Clock clock,
      PageParserFactory parserFactory,
      @Timeout Duration timeout,
      @PopularWordCount int popularWordCount,
      @TargetParallelism int threadCount,
      @MaxDepth int maxDepth,
      @IgnoredUrls List<Pattern> ignoredUrls) {
    this.clock = clock;
    this.parserFactory = parserFactory;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
    this.maxDepth = maxDepth;
    this.ignoredUrls = ignoredUrls;
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    Object visitedUrlsBlock = new Object();
    Object countsBlock = new Object();
    final CountDownLatch latch = new CountDownLatch(startingUrls.size());
    final Map<String, Integer> counts = Collections.synchronizedMap(new HashMap<>());
    final Set<String> visitedUrls = Collections.synchronizedSet(new HashSet<>());
    CrawlTask.Builder taskBuilder = new CrawlTask.Builder()
            .setDeadline(clock.instant().plus(timeout))
            .setClock(clock)
            .setIgnoredUrls(ignoredUrls)
            .setMaxDepth(maxDepth)
            .setPopularWordCount(popularWordCount)
            .setVisitedUrls(visitedUrls)
            .setCounts(counts)
            .setLatch(latch)
            .setParserFactory(parserFactory)
            .setVisitedUrlsBlock(visitedUrlsBlock)
            .setCountsBlock(countsBlock);

    List<CrawlTask> tasks = startingUrls.stream().map(url -> taskBuilder.setUrl(url).build()).toList();

    tasks.parallelStream().forEach(pool::execute);
    pool.shutdown();

    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    if (counts.isEmpty()) {
      return new CrawlResult.Builder()
              .setWordCounts(counts)
              .setUrlsVisited(visitedUrls.size())
              .build();
    }

    return new CrawlResult.Builder()
            .setWordCounts(WordCounts.sort(counts, popularWordCount))
            .setUrlsVisited(visitedUrls.size())
            .build();
  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }

  private static final class CrawlTask extends RecursiveAction {

    private final Clock clock;
    private final int popularWordCount;
    private final int maxDepth;
    private final List<Pattern> ignoredUrls;
    private final Map<String, Integer> counts;
    private final Set<String> visitedUrls;
    private final Instant deadline;
    private final String url;
    private final PageParserFactory parserFactory;
    private final Object visitedUrlsBlock;
    private final Object countsBlock;
    private final CountDownLatch latch;

    public CrawlTask(Clock clock,
                     Instant deadline,
                     int popularWordCount,
                     int maxDepth,
                     List<Pattern> ignoredUrls,
                     Map<String, Integer> counts,
                     Set<String> visitedUrls,
                     String url,
                     PageParserFactory parserFactory,
                     Object visitedUrlsBlock,
                     Object countsBlock,
                     CountDownLatch latch) {
      this.clock = clock;
      this.deadline = deadline;
      this.popularWordCount = popularWordCount;
      this.maxDepth = maxDepth;
      this.ignoredUrls = ignoredUrls;
      this.counts = counts;
      this.visitedUrls = visitedUrls;
      this.url = url;
      this.visitedUrlsBlock = visitedUrlsBlock;
      this.countsBlock = countsBlock;
      this.parserFactory = parserFactory;
      this.latch = latch;
    }

    @Override
    protected void compute() {

      if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
        latch.countDown();
        return;
      }
      for (Pattern pattern : ignoredUrls) {
        if (pattern.matcher(url).matches()) {
          latch.countDown();
          return;
        }
      }
      synchronized (visitedUrlsBlock) {
        if (visitedUrls.contains(url)) {
          latch.countDown();
          return;
        }
        visitedUrls.add(url);
      }

      PageParser.Result result = parserFactory.get(url).parse();

      synchronized (countsBlock){
      for (Map.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
        if (counts.containsKey(e.getKey())) {
          counts.put(e.getKey(), e.getValue() + counts.get(e.getKey()));
        } else {
          counts.put(e.getKey(), e.getValue());
        }
      }
      }

      CrawlTask.Builder taskBuilder = new CrawlTask.Builder()
              .setDeadline(deadline)
              .setClock(clock).setIgnoredUrls(ignoredUrls)
              .setMaxDepth(maxDepth-1)
              .setPopularWordCount(popularWordCount)
              .setVisitedUrls(visitedUrls)
              .setCounts(counts)
              .setVisitedUrlsBlock(visitedUrlsBlock)
              .setLatch(new CountDownLatch(1))
              .setCountsBlock(countsBlock)
              .setParserFactory(parserFactory);
      List<CrawlTask> tasks = result.getLinks().stream().map(url -> taskBuilder.setUrl(url).build()).toList();
      invokeAll(tasks);
      latch.countDown();
    }

    public static final class Builder {
      private Clock clock;
      private Instant deadline;
      private int popularWordCount;
      private int maxDepth;
      private List<Pattern> ignoredUrls;
      private Map<String, Integer> counts;
      private Set<String> visitedUrls;
      private String url;
      private PageParserFactory parserFactory;
      private Object visitedUrlsBlock;
      private Object countsBlock;
      private CountDownLatch latch;

      public CrawlTask build() {
        return new CrawlTask(
                clock,
                deadline,
                popularWordCount,
                maxDepth,
                ignoredUrls,
                counts,
                visitedUrls,
                url,
                parserFactory,
                visitedUrlsBlock,
                countsBlock,
                latch);
      }
      public Builder setLatch(CountDownLatch latch) {
        this.latch = latch;
        return this;
      }
      public Builder setUrl(String url) {
        this.url = url;
        return this;
      }

      public Builder setClock(Clock clock) {
        this.clock = clock;
        return this;
      }

      public Builder setPopularWordCount(int popularWordCount) {
        this.popularWordCount = popularWordCount;
        return this;
      }

      public Builder setMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
        return this;
      }

      public Builder setIgnoredUrls(List<Pattern> ignoredUrls) {
        this.ignoredUrls = ignoredUrls;
        return this;
      }

      public Builder setCounts(Map<String, Integer> counts) {
        this.counts = counts;
        return this;
      }

      public Builder setVisitedUrls(Set<String> visitedUrls) {
        this.visitedUrls = visitedUrls;
        return this;
      }

      public Builder setDeadline(Instant deadline) {
        this.deadline = deadline;
        return this;
      }

      public Builder setVisitedUrlsBlock(Object visitedUrlsBlock) {
        this.visitedUrlsBlock = visitedUrlsBlock;
        return this;
      }

      public Builder setCountsBlock(Object countsBlock) {
        this.countsBlock = countsBlock;
        return this;
      }
      public Builder setParserFactory(PageParserFactory parserFactory) {
        this.parserFactory = parserFactory;
        return this;
      }

    }
  }
}
