module FnordMetric::GaugeModifiers

  def set_max(gauge_name, value, uniq_name=nil)
    gauge = fetch_gauge(gauge_name)
    if gauge.three_dimensional?
      @redis.zrank(gauge.tick_key(time), uniq_name).callback do |ret|
      	@redis.zadd(gauge.tick_key(time), value, uniq_name).errback { |error| puts "Error: #{error}" } if ret.nil? || ret.to_i < value
      	@redis.incrby(gauge.tick_key(time, :count), value)
      end
    else
      @redis.hget(gauge.key, gauge.tick_at(time)).callback do |old|
        @redis.hset(gauge.key, gauge.tick_at(time), value) unless !(old.nil?) && old.to_i > value
      end
    end

  end

  def incr(gauge_name, value=1)
    gauge = fetch_gauge(gauge_name)
    assure_two_dimensional!(gauge)
    if gauge.unique? 
      incr_uniq(gauge, value)
    elsif gauge.average? 
      incr_avg(gauge, value)
    elsif gauge.calculate_per_request? 
      incr_per_request(gauge, value)
    else
      incr_tick(gauge, value)
    end
  end

  def incr_tick(gauge, value)
    if gauge.progressive?      
      @redis.incrby(gauge.key(:head), value).callback do |head|
        @redis.hsetnx(gauge.key, gauge.tick_at(time), head).callback do |_new|
          @redis.hincrby(gauge.key, gauge.tick_at(time), value) unless _new
        end
      end
    else
      @redis.hsetnx(gauge.key, gauge.tick_at(time), 0).callback do
        @redis.hincrby(gauge.key, gauge.tick_at(time), value)
      end
    end
  end  

  def incr_uniq(gauge, value, field_name=nil)
    return false if session_key.blank?
    @redis.sadd(gauge.tick_key(time, :sessions), session_key).callback do |_new|
      @redis.expire(gauge.tick_key(time, :sessions), gauge.tick)
      if (_new == 1) || (_new == true) #redis vs. em-redis
        @redis.incr(gauge.tick_key(time, :"sessions-count")).callback do |sc|
          field_name ? incr_field_by(gauge, field_name, value) : incr_tick(gauge, value)
        end
      end
    end
  end
  
  def incr_per_request(gauge, value)
    @redis.incr(gauge.key(:request)).callback do |request_count|
      @redis.incrby(gauge.key(:count), value).callback do |data_count|
        @redis.hsenx(gauge.key, gauge.tick_at(time), data_count.to_f / request_count.to_f) do |_new|
          if (_new == 1) || (_new == true)
            @redis.set(gauge.key(:request), 0)
            @redis.set(gauge.key(:count), 0)
          end
        end
      end
    end
  end

  def incr_avg(gauge, value)
    @redis.incr(gauge.tick_key(time, :"value-count")).callback do
      incr_tick(gauge, value)
    end
  end

  def incr_field(gauge_name, field_name, value=1)
    gauge = fetch_gauge(gauge_name)
    assure_three_dimensional!(gauge)
    if gauge.unique? 
      incr_uniq(gauge, value, field_name)
    else
      incr_field_by(gauge, field_name, value)
    end
  end

  def incr_field_by(gauge, field_name, value)
    @redis.zincrby(gauge.tick_key(time), value, field_name).callback do
      @redis.incrby(gauge.tick_key(time, :count), 1)
    end
  end  

  def set_value(gauge_name, value)
    gauge = fetch_gauge(gauge_name)
    assure_two_dimensional!(gauge)
    @redis.hset(gauge.key, gauge.tick_at(time), value)
  end

  def set_field(gauge_name, field_name, value)
    gauge = fetch_gauge(gauge_name)
    assure_three_dimensional!(gauge)
    @redis.zadd(gauge.tick_key(time), value, field_name)
  end


end
