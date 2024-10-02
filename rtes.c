#include <jansson.h>
#include <libwebsockets.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define QUEUESIZE 200
#define SYMBOL_COUNT 6
#define NUM_CONSUMERS 5

// Define your connection structure
typedef struct my_conn {
  lws_sorted_usec_list_t sul; // Schedule connection retry
  struct lws *wsi;            // WebSocket instance
  uint16_t retry_count;       // Consecutive retry attempts
} my_conn_t;

// Declare a global connection instance
static my_conn_t mco;

// Backoff array - contains incremental backoff delays
static const uint32_t backoff_ms[] = {1000, 1500, 2000, 2500, 3000, 3500,
                                      4000, 4500, 5000, 5500, 6000, 6500,
                                      7000, 7500, 8000, 8500, 9000, 9500};

// Configure retry policy
static const lws_retry_bo_t retry = {
    .retry_ms_table = backoff_ms,
    .retry_ms_table_count = LWS_ARRAY_SIZE(backoff_ms),
    .conceal_count = LWS_ARRAY_SIZE(backoff_ms),

    .secs_since_valid_ping = 3, // ping the server after being idle for 3 secs
    .secs_since_valid_hangup = 10, // hangup after 10 secs of being idle

    .jitter_percent = 20,
};

// Each trade consists of a price, a volume, the symbol index,
// as well as two timestamps
typedef struct {
  double price;
  double volume;
  time_t timestamp1;
  time_t timestamp2;
  int symbol_index;
} Trade;

// TradeData refers to all the trades that correspond to one symbol
// Includes a mutex for locking upon concurrent modification
typedef struct {
  Trade *trades;
  size_t trade_count;
  size_t trade_capacity;
  FILE *trades_file;
  FILE *candlestick_file;
  FILE *moving_avg_file;
  FILE *delays_file;
  double *delays;
  size_t delay_count;
  size_t delay_capacity;
  pthread_mutex_t mutex;
} TradeData;

// Queue - from prod/cons
// Describes the architecture of the queue containing the trades
typedef struct {
  Trade trades[QUEUESIZE];
  long head, tail;
  int full, empty;
  pthread_mutex_t *mut;
  pthread_cond_t *notFull, *notEmpty;
} queue;

// Declare the functions
queue *queueInit(void);
void queueDelete(queue *q);
void queueAdd(queue *q, Trade in);
void queueDel(queue *q, Trade *out);

void *consumer(void *args);
void *calculation_thread(void *arg);
static void write_trade_to_file(TradeData *trade_data, int index, double price,
                                double volume, time_t timestamp);
static void calculate_candlesticks(void);
static void calculate_moving_averages(void);
static void init_trade_data(void);
static void connect_client(lws_sorted_usec_list_t *sul);
int find_symbol_index(const char *symbol);

// Declare global variables
static int interrupted = 0;
static int connected = 0;
static int port = 443;
static struct lws_context *context;
static const char *address = "ws.finnhub.io";
static const char *path = "/?token=coudikpr01qhf5nregn0coudikpr01qhf5nregng";
// static const char *symbols[SYMBOL_COUNT] = {
//     "TSLA", "NVDA", "AAPL", "AMZN", "BINANCE:ETHUSDT", "BINANCE:BTCUSDT"};
static const char *symbols[SYMBOL_COUNT] = {
    "BINANCE:BTCUSDT", "BINANCE:ETHUSDT", "BINANCE:DOGEUSDT",
    "BINANCE:BNBUSDT", "BINANCE:DOTUSDT", "BINANCE:SOLUSDT"};
// static const char *symbols[SYMBOL_COUNT] = {"BINANCE:BTCUSDT",
//                                             "BINANCE:ETHUSDT",
//                                             "BINANCE:DOGEUSDT",
//                                             "BINANCE:SOLUSDT",
//                                             "NVDA",
//                                             "AAPL"};
static TradeData trade_data[SYMBOL_COUNT];

queue *fifo;

// Initialize the queue by allocating enough memory
// and setting the values to their default value
queue *queueInit(void) {
  queue *q;

  q = (queue *)malloc(sizeof(queue));
  if (q == NULL)
    return (NULL);

  q->empty = 1;
  q->full = 0;
  q->head = 0;
  q->tail = 0;
  q->mut = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
  pthread_mutex_init(q->mut, NULL);
  q->notFull = (pthread_cond_t *)malloc(sizeof(pthread_cond_t));
  pthread_cond_init(q->notFull, NULL);
  q->notEmpty = (pthread_cond_t *)malloc(sizeof(pthread_cond_t));
  pthread_cond_init(q->notEmpty, NULL);

  // Exit the program if the initialization fails
  if (q->mut == NULL || q->notFull == NULL || q->notEmpty == NULL) {
    fprintf(stderr,
            "Failed to allocate memory for mutex or condition variables\n");
    exit(1);
  }

  return q;
}

// Free the allocated memory, to release
// the captured resources upon deletion
void queueDelete(queue *q) {
  pthread_mutex_destroy(q->mut);
  free(q->mut);
  pthread_cond_destroy(q->notFull);
  free(q->notFull);
  pthread_cond_destroy(q->notEmpty);
  free(q->notEmpty);
  free(q);
}

// Enqueue a trade, after checking if it's possible
// Also set the condition according to the situation
void queueAdd(queue *q, Trade in) {
  q->trades[q->tail] = in;
  q->tail++;
  if (q->tail == QUEUESIZE)
    q->tail = 0;
  if (q->tail == q->head)
    q->full = 1;
  q->empty = 0;
}

// Dequeue a trade, after checking if it's possible
// Also set the condition according to the situation
void queueDel(queue *q, Trade *out) {
  *out = q->trades[q->head];
  q->head++;
  if (q->head == QUEUESIZE)
    q->head = 0;
  if (q->head == q->tail)
    q->empty = 1;
  q->full = 0;
}

// The consumer function to be assigned to the consumer threads
void *consumer(void *q) {
  queue *fifo = (queue *)q;
  Trade trade;

  while (1) {
    pthread_mutex_lock(fifo->mut);
    // Check if the queue is empty
    // or if the program has been interrupted by SIGINT
    while (fifo->empty && !interrupted) {
      lwsl_info("Consumer waiting, queue empty\n");
      // If so hold, don't skip the trade
      pthread_cond_wait(fifo->notEmpty, fifo->mut);
    }
    if (fifo->empty && interrupted) {
      pthread_mutex_unlock(fifo->mut);
      // Unlock mutex and exit the while loop -> graceful termination
      break;
    }
    // Dequeue a trade into the trade variable
    queueDel(fifo, &trade);
    // Signal that the queue is no longer full and unlock the queue mutex
    // to allow further additions to the queue
    pthread_cond_signal(fifo->notFull);
    pthread_mutex_unlock(fifo->mut);

    // Check if the trade symbol is valid, or else skip the trade
    if (trade.symbol_index < 0 || trade.symbol_index >= SYMBOL_COUNT) {
      fprintf(stderr, "Invalid symbol index: %d\n", trade.symbol_index);
      continue; // Skip processing this trade
    }

    // Lock the mutex of the trade data struct regarding the given symbol
    pthread_mutex_lock(&trade_data[trade.symbol_index].mutex);

    // If the data array for the given trade is full
    if (trade_data[trade.symbol_index].trade_count >=
        trade_data[trade.symbol_index].trade_capacity) {
      // Reallocate memory, using double the size
      size_t new_capacity = trade_data[trade.symbol_index].trade_capacity * 2;
      void *new_trades = realloc(trade_data[trade.symbol_index].trades,
                                 new_capacity * sizeof(Trade));
      // Check if an error occured, if so just skip the current trade
      if (new_trades == NULL) {
        fprintf(stderr, "Failed to resize trades array for symbol index: %d\n",
                trade.symbol_index);
        pthread_mutex_unlock(&trade_data[trade.symbol_index].mutex);
        continue; // Skip processing this trade
      }
      // Move the new trades (that's 2x the size) to the trades
      // variables of the trade data, same for new capacity
      trade_data[trade.symbol_index].trades = new_trades;
      trade_data[trade.symbol_index].trade_capacity = new_capacity;
    }

    // Add the current trade to the trade data trades field
    trade_data[trade.symbol_index]
        .trades[trade_data[trade.symbol_index].trade_count] = trade;
    // Increment the trade count index
    trade_data[trade.symbol_index].trade_count++;
    // Write the trade to file and unlock the respective symbol mutex
    write_trade_to_file(&trade_data[trade.symbol_index], trade.symbol_index,
                        trade.price, trade.volume, trade.timestamp2);
    pthread_mutex_unlock(&trade_data[trade.symbol_index].mutex);
  }
  return NULL;
}

// Cleanup file resources
void cleanup() {
  // For every symbol, check for null pointers
  // and then free the memory used by the files
  for (int i = 0; i < SYMBOL_COUNT; i++) {
    if (trade_data[i].trades) {
      free(trade_data[i].trades);
    }
    if (trade_data[i].delays) {
      free(trade_data[i].delays);
    }
    if (trade_data[i].trades_file) {
      fclose(trade_data[i].trades_file);
    }
    if (trade_data[i].candlestick_file) {
      fclose(trade_data[i].candlestick_file);
    }
    if (trade_data[i].moving_avg_file) {
      fclose(trade_data[i].moving_avg_file);
    }
    if (trade_data[i].delays_file) {
      fclose(trade_data[i].delays_file);
    }
    // Destroy the mutex
    pthread_mutex_destroy(&trade_data[i].mutex);
  }
}

// Initialize the necessary files and variables
void init_trade_data() {
  // Make directory, if it doesn't exist yet
  struct stat st = {0};
  if (stat("results", &st) == -1) {
    mkdir("results", 0700);
  }

  for (int i = 0; i < SYMBOL_COUNT; i++) {
    // Create the trades, candlestick, moving_avg and delays files
    char trades_filename[256], candlestick_filename[256],
        moving_avg_filename[256], delays_filename[256];

    snprintf(trades_filename, sizeof(trades_filename), "results/%s_trades.txt",
             symbols[i]);
    snprintf(candlestick_filename, sizeof(candlestick_filename),
             "results/%s_candlestick.txt", symbols[i]);
    snprintf(moving_avg_filename, sizeof(moving_avg_filename),
             "results/%s_moving_avg.txt", symbols[i]);
    snprintf(delays_filename, sizeof(delays_filename), "results/%s_delays.txt",
             symbols[i]);

    // Open them in append mode
    trade_data[i].trades_file = fopen(trades_filename, "a");
    trade_data[i].candlestick_file = fopen(candlestick_filename, "a");
    trade_data[i].moving_avg_file = fopen(moving_avg_filename, "a");
    trade_data[i].delays_file = fopen(delays_filename, "a");
    // Initialize the trade data mutex
    pthread_mutex_init(&trade_data[i].mutex, NULL);

    // Set initial values for capacities & counts and allocate initial memory
    trade_data[i].trade_capacity = 10000;
    trade_data[i].trade_count = 0;
    trade_data[i].trades = malloc(trade_data[i].trade_capacity * sizeof(Trade));
    trade_data[i].delay_capacity = 10000;
    trade_data[i].delay_count = 0;
    trade_data[i].delays =
        malloc(trade_data[i].delay_capacity * sizeof(double));

    // Exit on every possible initialization error
    if (!trade_data[i].trades_file || !trade_data[i].candlestick_file ||
        !trade_data[i].moving_avg_file || !trade_data[i].trades ||
        !trade_data[i].delays_file) {
      fprintf(stderr, "Failed to open files for symbol %s\n", symbols[i]);
      exit(1);
    }
  }
}

// Get the current time in UNIX timestamp (in microseconds)
time_t getNow() {
  struct timeval timestamp;
  gettimeofday(&timestamp, NULL);

  return (long long)(timestamp.tv_sec) * 1000LL + (timestamp.tv_usec / 1000LL);
}

// Write the given trade to its respective file
static void write_trade_to_file(TradeData *trade_data, int index, double price,
                                double volume, time_t timestamp) {
  // Use this format to write the trade to the file
  // Note: gcc will complain for the `lld` format specifier
  // the arm compiler wont
  fprintf(trade_data->trades_file,
          "Price: %.2f | Volume: %.4f | Timestamp: %lld\n", price, volume,
          timestamp);
  // Flush the buffer, ensuring everything is written immediately
  fflush(trade_data->trades_file);

  // Get the current time
  time_t timestamp2 = getNow();
  // Calculate the exact delay in milliseconds
  double delay = (double)(timestamp2 - timestamp);

  // Check if the delay count is full
  if (trade_data->delay_count < trade_data->delay_capacity) {
    trade_data->delays[trade_data->delay_count++] = delay;
  } else {
    lwsl_user("Delay array is full\n");
    // Resize the delay array to use double the size
    size_t new_capacity =
        trade_data->delay_capacity ? trade_data->delay_capacity * 2 : 1;
    double *new_delays =
        realloc(trade_data->delays, new_capacity * sizeof(double));
    if (new_delays == NULL) {
      lwsl_err("Failed to resize delays array!\n");
      return;
    }
    // Same way the trades are resized
    trade_data->delays = new_delays;
    trade_data->delay_capacity = new_capacity;
    // Add the new delay
    trade_data->delays[trade_data->delay_count++] = delay;
  }
}

// Write the average delays to the respective file
static void write_average_delays(TradeData *trade_data) {
  // Check if the delays file can be opened
  FILE *file = trade_data->delays_file;
  if (file == NULL) {
    perror("Error opening file");
    return;
  }

  // If no trades were recorded in the past 1 minute, don't write anything
  if (trade_data->delay_count == 0) {
    lwsl_user("No delays recorded.\n");
    return;
  }

  // Calculate the average delay per minute
  double total_delay = 0;
  for (size_t i = 0; i < trade_data->delay_count; i++) {
    total_delay += trade_data->delays[i];
  }

  double average_delay = total_delay / trade_data->delay_count;
  // Write and flush using this format
  fprintf(file, "Average delay per minute: %.2f milliseconds\n", average_delay);
  fflush(file);

  // Reset the delay count that serves as an index too
  // in order to overwrite the array on the next call
  trade_data->delay_count = 0;
}

static void calculate_candlesticks() {
  for (int i = 0; i < SYMBOL_COUNT; i++) {
    // Lock the symbol mutex and initialize variables
    pthread_mutex_lock(&trade_data[i].mutex);
    time_t now = getNow();
    double high = -9999999999;
    double low = 9999999999;
    double volume = 0.0;
    int first_trade_index = -1;
    int last_trade_index = -1;

    // Iterate through the trades array on reverse, filtering out
    // the trades that weren't recorded in the last minute
    for (int j = trade_data[i].trade_count - 1; j >= 0; j--) {
      if ((now - trade_data[i].trades[j].timestamp1) <= 60000) {
        if (first_trade_index == -1) {
          first_trade_index = j;
        }
        last_trade_index = j;
        double price = trade_data[i].trades[j].price;
        // Find the highest price during the last minute
        if (price > high)
          high = price;
        // Find the lowest price during the last minute
        if (price < low)
          low = price;
        volume += trade_data[i].trades[j].volume;
      } else {
        break;
      }
    }

    // If the first_trade_index & last_trade_index are valid (not -1)
    // it means that trades were indeed recorded within the last minute
    if (first_trade_index != -1 && last_trade_index != -1) {
      // The trade on last_trade_index index is the open price, since
      // the loop was iterating on reverse
      // thus first_trade_index refers to the close price
      double open = trade_data[i].trades[last_trade_index].price;
      double close = trade_data[i].trades[first_trade_index].price;

      // Write using this format and flush
      fprintf(
          trade_data[i].candlestick_file,
          "Open: %.2f | Close: %.2f | High: %.2f | Low: %.2f | Volume: %.4f\n",
          open, close, high, low, volume);
      fflush(trade_data[i].candlestick_file);
    } else {
      lwsl_user("No candlestick recorded for %s", symbols[i]);
    }

    pthread_mutex_unlock(&trade_data[i].mutex);
  }
}

static void calculate_moving_averages() {
  for (int i = 0; i < SYMBOL_COUNT; i++) {
    // Lock the mutex of the respective symbol
    pthread_mutex_lock(&trade_data[i].mutex);

    // If there are no trades for the current symbol, unlock the mutex and
    // continue to the next one
    if (trade_data[i].trade_count == 0) {
      pthread_mutex_unlock(&trade_data[i].mutex);
      lwsl_user("No moving average recorded for %s", symbols[i]);
      continue;
    }

    // `sumPrices` accumulates the sum of the prices of trades that occured in
    // the last 15 minutes
    double sumPrices = 0.0;
    // `sumVolumes` accumulates the sum of the volumes of trades that occured in
    // the last 15 minutes
    double sumVolumes = 0.0;
    // Tracks the number of trades that occured in the last 15 minutes
    int count = 0;
    // The current time is captured here
    time_t now = getNow();

    // Iterate through the trades array on reverse, filtering out
    // the trades that weren't recorded in the last 15 minutes
    for (int j = trade_data[i].trade_count - 1; j >= 0; j--) {
      if ((now - trade_data[i].trades[j].timestamp1) <= 900000) { // 15 minutes
        // Increment `sumPrices` by the trades price field
        sumPrices += trade_data[i].trades[j].price;
        // Increment `sumVolumes` by the trades volume field
        sumVolumes += trade_data[i].trades[j].volume;
        // Increment the count by 1
        count++;

        // If not, break
      } else {
        break;
      }
    }

    // If there were trades in the last 15 minutes
    if (count > 0) {
      // Calculate the moving average by dividing the `sumPrices` by the `count`
      double movingAvgPrice = sumPrices / count;
      // Write using this format and flush
      fprintf(trade_data[i].moving_avg_file,
              "Moving Avg Price: %.2f | Total Volume: %.4f\n", movingAvgPrice,
              sumVolumes);
      fflush(trade_data[i].moving_avg_file);
    }
    // Unlock the mutex
    pthread_mutex_unlock(&trade_data[i].mutex);
  }
}

// Remove the trades in the array that no longer
// serve any purpose
void remove_old_trades(TradeData *data) {
  pthread_mutex_lock(&data->mutex);

  time_t now = getNow();
  size_t new_count = 0;

  // Iterate through the trades array on reverse, filtering out
  // the trades that weren't recorded in the last 15 minutes
  for (size_t i = 0; i < data->trade_count; i++) {
    if ((now - data->trades[i].timestamp1) <=
        900000) { // 15 minutes = 900000 milliseconds
      // Overwrite the array with relevant trades only
      data->trades[new_count++] = data->trades[i];
    }
  }

  // Update the trade count
  data->trade_count = new_count;

  pthread_mutex_unlock(&data->mutex);
}

// Recording function that will run in parallel
// to trigger actions
void *calculation_thread(void *arg) {
  int count = 0;
  // Check if the program is not interrupted
  while (!interrupted) {
    // It must trigger every minute
    sleep(60);

    // For every trade symbol, execute the following
    // functions every minute
    for (int i = 0; i < SYMBOL_COUNT; i++) {
      remove_old_trades(&trade_data[i]);
      write_average_delays(&trade_data[i]);
    }

    // Calculate candlesticks runs every minute
    // whereas calculate moving averages runs every minute
    // after the first 14 minutes
    calculate_candlesticks();
    if (count >= 14) {
      calculate_moving_averages();
    } else {
      count++;
    }
  }
  return NULL;
}

// Helper function to check if a symbol exists
// given the symbol character, returns the index in the array
int find_symbol_index(const char *symbol) {
  for (int i = 0; i < SYMBOL_COUNT; i++) {
    if (strcmp(symbols[i], symbol) == 0) {
      return i;
    }
  }
  return -1;
}

// The callback acts as a multiplexer on the server responses
static int lws_callback(struct lws *wsi, enum lws_callback_reasons reason,
                        void *user, void *in, size_t len) {

  my_conn_t *m = (my_conn_t *)user;

  switch (reason) {
  // Triggered when a successful connection is established
  // Sends a subscribe message to the wsi for each symbol
  case LWS_CALLBACK_CLIENT_ESTABLISHED:
    connected = 1;
    lwsl_user("Client has connected\n");
    m->retry_count = 0;

    for (int i = 0; i < SYMBOL_COUNT; i++) {
      char msg[256];
      snprintf(msg, sizeof(msg), "{\"type\":\"subscribe\",\"symbol\":\"%s\"}",
               symbols[i]);
      unsigned char buf[LWS_PRE + strlen(msg)];
      memcpy(&buf[LWS_PRE], msg, strlen(msg));
      lws_write(wsi, &buf[LWS_PRE], strlen(msg), LWS_WRITE_TEXT);
      lwsl_user("Sent subscription message: %s\n", msg);
    }
    break;

  // Triggered when an actual information message is received
  case LWS_CALLBACK_CLIENT_RECEIVE: {
    json_t *root;
    json_error_t error;

    // Unload the json string
    root = json_loads((char *)in, 0, &error);
    if (!root) {
      lwsl_err("JSON parse error: on line %d: %s\n", error.line, error.text);
      break;
    }

    // Find the value of the key "type"
    json_t *type = json_object_get(root, "type");
    if (json_is_string(type)) {
      const char *value = json_string_value(type);

      // If the type is trade, get its data field
      if (strcmp(value, "trade") == 0) {
        json_t *data = json_object_get(root, "data");
        if (json_is_array(data)) {
          size_t index;
          json_t *value;
          // Get the symbol, price, volume and timestamp from the json object
          json_array_foreach(data, index, value) {
            const char *symbol = json_string_value(json_object_get(value, "s"));
            double price = json_number_value(json_object_get(value, "p"));
            double volume = json_number_value(json_object_get(value, "v"));
            // Refers to the timestamp when the trade was sent
            time_t timestamp1 = json_integer_value(json_object_get(value, "t"));
            // Refers to the timestamp when the trade was received by the
            // program
            time_t timestamp2 = getNow();

            // Find the symbol index in the global definition array
            int symbol_index = find_symbol_index(symbol);

            // If it's a valid symbol
            if (symbol_index >= 0) {
              // Create a trade
              Trade trade = {price, volume, timestamp1, timestamp2,
                             symbol_index};

              // Enqueue the trade, by locking the mutex first
              pthread_mutex_lock(fifo->mut);
              while (fifo->full) {
                lwsl_info("Producer waiting, queue full\n");
                // Hold, until the queue is not full
                pthread_cond_wait(fifo->notFull, fifo->mut);
              }
              queueAdd(fifo, trade);
              // Signal the notEmpty condition since a trade was just enqueued
              pthread_cond_signal(fifo->notEmpty);
              // Unlock the mutex
              pthread_mutex_unlock(fifo->mut);
            }
          }
        }
      }

      // If the message sent was of type ping, respond with pong
      else if (strcmp(value, "ping") == 0) {
        if (!connected) {
          char pong_msg[256];
          snprintf(pong_msg, sizeof(pong_msg), "{\"type\":\"pong\"}");
          lwsl_user("Pinged\n");
          // If the program wasn't interrupted
          if (!interrupted) {
            lwsl_warn("Attempting to reconnect\n");
            sleep(10);
            // Try to reconnect using the connect_client function via:
            if (lws_retry_sul_schedule_retry_wsi(wsi, &m->sul, connect_client,
                                                 &m->retry_count)) {
              lwsl_err("Connection attempts exhausted\n");
              interrupted = 1;
              pthread_cond_broadcast(fifo->notEmpty);
              pthread_cond_broadcast(fifo->notFull);
            }
            return 0;
          }
        }
        // Handle the rest of the possible error occations
      } else if (strcmp(value, "error") == 0) {
        lwsl_err("Got an error\n");
        connected = 0;
      } else {
        lwsl_err("I dont know: %s\n", value);
      }
    } else {
      lwsl_err("Type ???\n");
    }

    // Release the reference to the json object
    json_decref(root);
  } break;

  // Triggered when the connection is closed
  case LWS_CALLBACK_CLIENT_CLOSED:
    lwsl_err("Connection closed\n");
    connected = 0;
    // The same reconnect approach as above
    if (!interrupted) {
      lwsl_warn("Attempting to reconnect\n");
      sleep(10);
      if (lws_retry_sul_schedule_retry_wsi(wsi, &m->sul, connect_client,
                                           &m->retry_count)) {
        lwsl_err("Connection attempts exhausted\n");
        interrupted = 1;
        pthread_cond_broadcast(fifo->notEmpty);
        pthread_cond_broadcast(fifo->notFull);
      }
      return 0;
    }
    break;
  // Triggered when there is a connection error (e.g. Network error)
  case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
    lwsl_err("Connection error: %s\n", in ? (char *)in : "(null)");
    connected = 0;
    // The same reconnect approach as above
    if (!interrupted) {
      lwsl_warn("Attempting to reconnect\n");
      sleep(10);
      if (lws_retry_sul_schedule_retry_wsi(wsi, &m->sul, connect_client,
                                           &m->retry_count)) {
        lwsl_err("Connection attempts exhausted\n");
        interrupted = 1;
        pthread_cond_broadcast(fifo->notEmpty);
        pthread_cond_broadcast(fifo->notFull);
      }
      return 0;
    }
    break;

  // Triggered when the server sends a ping signal
  // Could be used for logging, but it turns to spam (default is 5m)
  case LWS_CALLBACK_CLIENT_RECEIVE_PONG:
    break;

  // Exit if something else is sent
  default:
    break;
  }

  return lws_callback_http_dummy(wsi, reason, user, in, len);
}

// Define the WebSocket server supported protocols
static const struct lws_protocols protocols[] = {
    {
        "my-protocol", // Protocol name - used for logging identification
        lws_callback,  // Callback function that will handle the events
        0,     // The amount of memory (in bytes) to allocate for each WebSocket
               // connection
        65536, // The size of the receive buffer for the protocol, in this case
               // 64KB
    },
    // Special struct that marks the end of the protocols array
    {NULL, NULL, 0, 0}};

static void connect_client(lws_sorted_usec_list_t *sul) {
  // A pointer to the connection instance
  my_conn_t *m = lws_container_of(sul, my_conn_t, sul);
  struct lws_client_connect_info i;

  // Initialize the required fields of the lws connection
  memset(&i, 0, sizeof(i));
  i.context = context; // Defined globally - assigned once by the main function
  i.port = port;       // Defined globally
  i.address = address; // Defined globally
  i.path = path;       // Defined globally
  i.host = address;
  i.origin = address;
  i.ssl_connection = LCCSCF_USE_SSL; // Use ssl encryption for the connection
  i.protocol = protocols[0].name;    // The self defined protocol name
  i.local_protocol_name = protocols[0].name;
  i.pwsi = &m->wsi;                 // The websocket instance
  i.retry_and_idle_policy = &retry; // The retry policy defined globally
  i.userdata = m;                   // The connection instance

  // Use the above fields to connect to the Finnhub API
  if (!lws_client_connect_via_info(&i)) {
    lwsl_err("Client connection failed\n");
    // Schedule a retry
    if (lws_retry_sul_schedule(context, 0, sul, &retry, connect_client,
                               &m->retry_count)) {
      lwsl_err("Connection attempts exhausted\n");
      interrupted = 1;
    }
  }
}

// When the SIGINT signal is caught, set the interrupted flag to 1
// This exits the Main Loop
void sigint_handler(int sig) {
  interrupted = 1;
  printf("\n");
  pthread_cond_broadcast(fifo->notEmpty);
  pthread_cond_broadcast(fifo->notFull);
}

int main() {
  pthread_t con[NUM_CONSUMERS], calc_thread;

  // Initialize the queue
  fifo = queueInit();
  if (fifo == NULL) {
    fprintf(stderr, "Queue Init failed.\n");
    exit(1);
  }

  // Handle the SIGINT signal
  signal(SIGINT, sigint_handler);

  // Used to configure and create context; a state in which LWS operates
  struct lws_context_creation_info info;

  // Initializes the info structure to zero
  memset(&info, 0, sizeof info);
  // This context will not be listening; it's a client
  info.port = CONTEXT_PORT_NO_LISTEN;
  // Sets the protocols list
  info.protocols = protocols;
  // Initializes global SSL libraries
  info.options = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;

  // Sets the log level to only report errors
  lws_set_log_level(LLL_ERR | LLL_USER | LLL_WARN, NULL);
  // Creates a new LWS context using the defined info structure
  context = lws_create_context(&info);
  // If the context creation fails, exit the program
  if (!context) {
    lwsl_err("lws init failed\n");
    return 1;
  }

  // File initialization
  init_trade_data();

  // Schedule a connection - uses the context defined above
  lws_sul_schedule(context, 0, &mco.sul, connect_client, 1);

  // Create a total of NUM_CONSUMERS consumer threads
  // and assign the consumer function
  for (int i = 0; i < NUM_CONSUMERS; i++) {
    pthread_create(&con[i], NULL, consumer, fifo);
  }
  // Create a calculation thread and assign the function with the same name
  pthread_create(&calc_thread, NULL, calculation_thread, NULL);

  // Main event loop for processing WebSocket events as long as there are no
  // errors (n >= 0) and the interrupted flag is not set.
  int n = 0;
  while (n >= 0 && !interrupted) {
    n = lws_service(context, 0);
  }

  // Wait for consumers and calculation threads to finish
  for (int i = 0; i < NUM_CONSUMERS; i++) {
    pthread_join(con[i], NULL);
  }

  // Execute the resource deallocation functions
  queueDelete(fifo);
  cleanup();
  // After the loops exits, the context is destroyed to clean up resources
  lws_context_destroy(context);

  return 0;
}
