# TikFlow - Ghi chú kỹ thuật phỏng vấn

Tài liệu này tóm tắt các điểm kỹ thuật có thể dùng khi phỏng vấn: sơ đồ kiến trúc, luồng dữ liệu, những đoạn code thể hiện cách xử lý bất đồng bộ, retry/backoff, realtime cache update và Clean Architecture. Các domain, token, API key và cấu hình nhạy cảm đã được ẩn hoặc thay bằng tên trung lập.

## 1. System Overview

TikFlow là hệ thống quản lý TikTok Shop gồm ba phần chính:

- **Next.js frontend**: dashboard quản lý đơn hàng, sản phẩm, shop, tài chính, chấm công; dùng React Query để cache data và STOMP/SockJS để nhận realtime update.
- **Spring Boot backend**: REST API, auth/team/RBAC, module order/product/finance, TikTok Shop integration, WebSocket notification, Redis rate limit/cache, PostgreSQL persistence.
- **Go ADMS service**: nhận dữ liệu máy chấm công qua giao thức ADMS/iClock HTTP, parse log, đẩy Kafka, worker xử lý chấm công, DLQ khi lỗi.

## 2. Architecture Diagram

![System Architecture](docs/interview-assets/system-architecture.svg)

Luồng chính:

1. Browser gọi Next.js UI; Next.js gọi backend qua Axios REST client.
2. Backend Spring Boot xử lý auth, shop, order, product, finance và gọi TikTok Shop API qua client có rate limit/token refresh.
3. Redis dùng cho cache/rate limit, PostgreSQL lưu dữ liệu nghiệp vụ, Kafka chuyển event bất đồng bộ.
4. Go ADMS service nhận dữ liệu từ hardware, parse và đẩy event chấm công vào Kafka.
5. Backend đẩy notification về client qua STOMP/SockJS.

## 3. ADMS / Hardware Data Flow

![ADMS Data Flow](docs/interview-assets/adms-data-flow.svg)

Ghi chú khi phỏng vấn: code hiện tại **không mở raw TCP socket trực tiếp với máy chấm công**. Máy ADMS/iClock giao tiếp với Go service qua các HTTP endpoint như `/:tenant/iclock/cdata`, `/:tenant/iclock/getrequest`, `/:tenant/iclock/devicecmd`. Phần "socket" trong hệ thống nằm ở kết nối nội bộ Kafka TCP và WebSocket/STOMP giữa backend với client.

## 4. Screenshots

![img.png](img.png)
![img_1.png](img_1.png)
![img_2.png](img_2.png)
![img_3.png](img_3.png)
![img_4.png](img_4.png)
![img_5.png](img_5.png)

## 5. Code Snippets

### 5.1 Next.js API client - refresh token queue


Điểm đáng nói: khi nhiều request cùng nhận 401, chỉ một request được refresh token; các request còn lại chờ trong queue để tránh refresh storm.

```ts
let isRefreshing = false;
let failedQueue: QueuedRequest[] = [];

const processQueue = (error: unknown) => {
  failedQueue.forEach((p) => {
    if (error) p.reject(error);
    else p.resolve(null);
  });
  failedQueue = [];
};

apiClient.interceptors.response.use(
  (response) => response,
  async (error: AxiosError) => {
    const originalRequest = error.config as InternalAxiosRequestConfig & {
      _retry?: boolean;
    };

    if (!originalRequest || error.response?.status !== 401) {
      return Promise.reject(error);
    }

    if (originalRequest._retry) {
      tokenService.clearAll();
      window.location.href = "/login";
      return Promise.reject(error);
    }
    originalRequest._retry = true;

    if (isRefreshing) {
      return new Promise((resolve, reject) => {
        failedQueue.push({
          resolve: () => resolve(apiClient(originalRequest)),
          reject,
        });
      });
    }

    isRefreshing = true;
    try {
      const data = await authService.refresh();
      tokenService.setTokens(
        data.accessToken,
        data.expiresIn,
        data.refreshToken,
        data.refreshExpiresIn
      );
      processQueue(null);
      return apiClient(originalRequest);
    } catch (refreshError) {
      processQueue(refreshError);
      tokenService.clearAll();
      window.location.href = "/login";
      return Promise.reject(refreshError);
    } finally {
      isRefreshing = false;
    }
  }
);
```

### 5.2 Realtime order update - patch React Query cache


Điểm đáng nói: client không reload toàn bộ danh sách order; websocket payload chỉ patch vào các query cache đang có, sau đó invalidate tab count nếu status thay đổi.

```tsx
const applyRealtimePatch = (order: Order, patch: Partial<Order>): Order => {
  const next = { ...order } as Record<string, unknown>;

  for (const key of Object.keys(patch) as Array<keyof Order>) {
    if (key === "items" || key === "id" || key === "shopId") continue;

    const value = patch[key];
    if (value && typeof value === "object" && !Array.isArray(value)) {
      next[key] = {
        ...(typeof next[key] === "object" ? next[key] as Record<string, unknown> : {}),
        ...(value as Record<string, unknown>),
      };
      continue;
    }
    next[key] = value as unknown;
  }

  return next as unknown as Order;
};

const updateOrderCache = (queryClient, orderId: string, payload: Record<string, unknown>) => {
  const allKeys = queryClient.getQueriesData<OrderCache | undefined>({
    queryKey: orderKeys.all,
  });

  for (const [key, cached] of allKeys) {
    if (!cached?.pages?.length) continue;

    const pages = cached.pages.map((page) => {
      const idx = page.items.findIndex((item) => item.id === orderId);
      if (idx === -1) return page;

      const items = [...page.items];
      items[idx] = applyRealtimePatch(items[idx], payload);
      return { ...page, items };
    });

    queryClient.setQueryData(key, { ...cached, pages });
  }
};
```

### 5.3 Spring Boot TikTok client - token refresh, rate limit, retry


Điểm đáng nói: tách logic cross-cutting của TikTok API thành một gateway: lấy token hợp lệ, acquire rate limiter, retry khi rate limit, force refresh token khi hết hạn.

```java
public <T> T execute(ShopDTO shop, Function<String, T> sdkCall) {
    String token = tokenProvider.getValidToken(shop);
    int maxRetries = 3;
    long backoffMs = 1000;

    RRateLimiter rateLimiter = rateLimiterManager.getRateLimiter(shop.getId());

    for (int attempt = 1; attempt <= maxRetries; attempt++) {
        boolean allowed = rateLimiter.tryAcquire(1);

        try {
            if (!allowed) {
                throw new TikTokRateLimitException(429, "Local Redis limit", "LOCAL_REDIS_LIMIT");
            }
            return sdkCall.apply(token);

        } catch (TikTokShopInativeException inactive) {
            tokenProvider.inactiveShop(shop);
            if (onEvict != null) onEvict.run();
            throw inactive;

        } catch (TikTokTokenExpiredException | TikTokUnauthorizedException expired) {
            token = tokenProvider.forceRefreshToken(shop);
            return sdkCall.apply(token);

        } catch (TikTokRateLimitException limited) {
            if (attempt == maxRetries) throw limited;
            sleep(backoffMs);
            backoffMs *= 2;
        }
    }

    throw new IllegalStateException("TikTok API call failed unexpectedly");
}
```

### 5.4 Product re-up - upload media in parallel

Điểm đáng nói: re-up product cần upload nhiều media độc lập. `CompletableFuture` giúp upload ảnh chính, ảnh SKU, size chart, video song song rồi mới build `CreateProductRequest`.

```java
CompletableFuture<Map<String, String>> mainImageFuture =
        CompletableFuture.supplyAsync(() -> buildImageUriMap(sourceProduct.getMainImages(), targetShop));

CompletableFuture<Map<String, String>> skuImageFuture =
        CompletableFuture.supplyAsync(() -> buildSkuImageUriMap(sourceProduct.getSkus(), targetShop));

CompletableFuture<String> sizeChartFuture =
        CompletableFuture.supplyAsync(() -> uploadSizeChartImage(sourceProduct.getSizeChart(), targetShop));

CompletableFuture<Video> videoFuture =
        CompletableFuture.supplyAsync(() -> uploadVideo(sourceProduct.getVideo(), targetShop));

CompletableFuture
        .allOf(mainImageFuture, skuImageFuture, sizeChartFuture, videoFuture)
        .join();

CreateProductRequest request = productDTOMapper.toCreateProductRequest(
        sourceProduct,
        locale,
        saveMode,
        mainImageFuture.join(),
        skuImageFuture.join(),
        warehouseId,
        sizeChartFuture.join(),
        categoryVersion,
        videoFuture.join()
);
```

### 5.5 Go ADMS worker - partition workers, backoff, DLQ

Điểm đáng nói: worker theo partition, fetch message có exponential backoff, commit sau khi xử lý; lỗi nghiệp vụ đẩy sang DLQ có retry.

```go
func (c *AttendanceConsumer) startAttendanceWorkers(ctx context.Context) {
    partitions := c.kafkaClient.GetPartitions(admskafka.TopicAttendanceParsed)
    if c.workerCount > 0 {
        partitions = make([]int, c.workerCount)
        for i := range partitions {
            partitions[i] = i
        }
    }

    for _, partition := range partitions {
        reader := c.kafkaClient.CreatePartitionReader(
            admskafka.TopicAttendanceParsed,
            partition,
        )

        c.attendanceWg.Add(1)
        go c.worker(ctx, reader, partition)
    }
}

func (c *AttendanceConsumer) worker(ctx context.Context, reader *kafkago.Reader, workerID int) {
    defer c.attendanceWg.Done()

    backoff := backoff.NewExponentialBackOff()
    backoff.InitialInterval = 100 * time.Millisecond
    backoff.MaxInterval = 30 * time.Second
    backoff.MaxElapsedTime = 0

    for {
        msg, err := reader.FetchMessage(ctx)
        if err != nil {
            if ctx.Err() != nil {
                return
            }
            select {
            case <-time.After(backoff.NextBackOff()):
                backoff.Reset()
                continue
            case <-ctx.Done():
                return
            }
        }

        backoff.Reset()
        c.processAttendance(ctx, msg, workerID)
        _ = reader.CommitMessages(ctx, msg)
    }
}
```

### 5.6 Go ADMS parser strategy - raw device data to Kafka messages

Điểm đáng nói: parser tách riêng theo device type. Handler không cần biết format raw của từng máy, chỉ nhận parser interface và đẩy batch message sang Kafka.

```go
type AttendanceHandler struct {
    Repo        repositories.AttendanceRepository
    Parser      IAttendanceParser
    KafkaClient *kafka.Client
}

func (h *AttendanceHandler) Handle(ctx context.Context, sn string, teamID string, rawData string) error {
    logs, err := h.Parser.Parse(sn, rawData, teamID)
    if err != nil {
        return err
    }
    if len(logs) == 0 {
        return nil
    }

    messages := make([]kafka.Message, 0, len(logs))
    for _, logData := range logs {
        messages = append(messages, kafka.Message{
            Topic: kafka.TopicAttendanceParsed,
            Key:   logData.EmployeeCode,
            Value: logData,
        })
    }

    return h.KafkaClient.Push(ctx, messages...)
}
```

### 5.7 Clean Architecture - use case publishes domain event through port

Điểm đáng nói: application use case không phụ thuộc trực tiếp vào Kafka/task queue. Nó publish qua domain port, infrastructure adapter mới map sang task cũ. Cách này giúp migrate dần theo Strangler Fig.

```java
@Service
@RequiredArgsConstructor
public class SyncTikTokOrdersUseCase {

    private final ShopPort shopPort;
    private final EventPublisher eventPublisher;
    private final OrderRepository orderRepository;

    public void executeSingle(OrderCommands.SyncSingleOrderCommand cmd) {
        String teamId = shopPort.getShopTeamId(cmd.shopId());
        boolean isNew = !orderRepository.existsById(new OrderId(cmd.orderId()));

        OrderDomainEvent.OrderRequest event = new OrderDomainEvent.OrderRequest(
                UUID.randomUUID().toString(),
                cmd.orderId(),
                cmd.shopId(),
                teamId,
                TimeSystem.getEpochSecond(),
                isNew,
                cmd.souceTrace()
        );

        eventPublisher.publishToApiThrottling(event);
    }
}

@Component
@RequiredArgsConstructor
public class EventPublisherAdapter implements EventPublisher {

    private final TaskProducer taskProducer;

    @Override
    public void publishToApiThrottling(OrderDomainEvent.OrderRequest event) {
        ApiRequestTask task = new ApiRequestTask();
        task.setTaskId(UUID.randomUUID().toString());
        task.setTaskType(ApiTaskType.TIKTOK_SINGER_ORDER_FETCH);
        task.setShopId(event.shopId());
        task.setTeamId(event.teamId());
        task.setPriority(true);
        task.setRetryCount(0);

        taskProducer.publishNewTask(task);
    }
}
```

## 6. Suggested Interview Talking Points

- **Reliability**: Kafka partition workers, commit sau khi process, DLQ khi lỗi, retry/backoff cho TikTok API và Kafka DLQ.
- **Performance**: upload media product song song bằng `CompletableFuture`, React Query patch cache thay vì reload toàn bộ list.
- **Security**: access token để trong memory/cookie, refresh flow có queue; WebSocket có auth header và hard lock khi token invalid.
- **Maintainability**: order module có domain/application/infrastructure boundary; TikTok client gom rate limit/token refresh vào một gateway.
- **Hardware integration**: ADMS/iClock không phải browser flow; Go service validate tenant/device, parse raw tab-delimited ATTLOG, đẩy sang Kafka để tách ingestion khỏi business processing.
