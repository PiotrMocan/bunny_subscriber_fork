module BunnySubscriber
  class Queue
    attr_reader :channel, :queue_consummer

    def initialize(channel)
      @channel = channel
      @channel.prefetch(101)
      @batch   = []
    end

    def subscribe(consumer)
      queue = create_queue(consumer)
      @queue_consummer = queue.subscribe(
        manual_ack: true,
        block: false
      ) do |delivery_info, properties, payload|
        if @batch.size < 100 && queue.message_count > 200
          @batch << [payload]
        elsif @batch.size > 0 && queue.message_count < 200
          consumer.event_process_around_action(
            @batch
          )
          @batch = []
        elsif @batch.size >= 100
          consumer.event_process_around_action(
            @batch
          )
          @batch = []
        else
          consumer.event_process_around_action(
            [payload]
          )
        end
        channel.acknowledge(delivery_info.delivery_tag, false)
      end
    end

    def unsubscribe
      return if @queue_consummer.cancel

      # If can cancel the consumer, try again
      sleep(1)
      unsubscribe
    end

    # def message(delivery_info, properties, payload)
    #   {
    #     delivery_info: delivery_info,
    #     properties: properties,
    #     payload: payload
    #   }
    # end

    private

    def create_queue(consumer)
      if consumer.subscriber_options[:queue_name].nil?
        raise ArgumentError, '`queue_name` option is required'
      end

      options = { durable: true }
      if (dl_exchange = consumer.subscriber_options[:dead_letter_exchange])
        options[:arguments] = { 'x-dead-letter-exchange': dl_exchange }
      end

      channel.queue(
        consumer.subscriber_options[:queue_name], options
      )
    end
  end
end
