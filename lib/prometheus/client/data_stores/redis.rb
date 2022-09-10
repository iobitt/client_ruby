require 'redis'

module Prometheus
  module Client
    module DataStores
      # Stores all the data in a Redis database
      class Redis
        class InvalidStoreSettingsError < StandardError; end

        attr_reader :connection_config

        def initialize(connection_config = {})
          @connection_config = connection_config
        end

        def for_metric(metric_name, metric_type:, metric_settings: {})
          MetricStore.new(metric_name, connection_config)
        end

        private

        class MetricStore
          attr_reader :metric_name, :redis

          def initialize(metric_name, connection_config)
            @metric_name = metric_name
            @redis = ::Redis.new(connection_config)
          end

          def synchronize
            yield
          end

          def set(labels:, val:)
            redis.hset(key, { encode_labels(labels) => val.to_f })
          end

          def increment(labels:, by: 1)
            redis.hincrby(key, encode_labels(labels), by)
          end

          def get(labels:)
            redis.hget(key, encode_labels(labels))
          end

          def all_values
            redis.hgetall(key).map { |key, value| [decode_labels(key), value] }
          end

          def key
            "metrics:#{metric_name}"
          end

          def encode_labels(labels)
            labels.map { |key, value| "#{key}=#{value}" }.join('&')
          end

          def decode_labels(labels)
            labels.split('&').map { |label| label.split('=') }.to_h
          end
        end

        private_constant :MetricStore
      end
    end
  end
end
