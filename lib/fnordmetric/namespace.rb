class FnordMetric::Namespace
  
  attr_reader :handlers, :gauges, :opts, :key, :dashboards

  @@opts = [:event, :gauge, :widget, :set_title, :active_users_available, :purge_after, :list_max_length]

  def initialize(key, opts)    
    @gauges = Hash.new
    @dashboards = Hash.new
    @handlers = Hash.new
    @title = key
    @active_users_available = true
    @opts = opts
    @key = key      
  end

  def ready!(redis)
    @redis = redis
    @gauges.map{ |k,g| g.add_redis(@redis) }
    self
  end

  def announce(event)                  
    announce_to_timeline(event)
    announce_to_typelist(event)
    
    if event[:_session]
      event[:_session_key] = announce_to_session(event).session_key 
    end
           
    purge_old_timeline(event[:time] - @opts[:purge_after]) if rand(100) < 100 && @opts[:purge_after]
       
    purge_old_typelist(event[:_type], @opts[:list_max_length]) if (rand(100) < 100) && @opts[:list_max_length]

    [
      @handlers[event[:_type].to_s],
      @handlers["*"]
    ].flatten.compact.each do |context| 
      context.call(event, @redis) 
    end

    self
  end

  def announce_to_session(event)
    FnordMetric::Session.create(@opts.clone.merge(
      :namespace_key => @key, 
      :namespace_prefix => key_prefix,
      :redis => @redis,
      :event => event
    )) 
  end

  def announce_to_timeline(event)
    timeline_key = key_prefix(:timeline)
    @redis.zadd(timeline_key, event[:_time], event[:_eid])
  end

  def purge_old_timeline(timestamp)
    @redis.zremrangebyscore(key_prefix(:timeline), '0', timestamp.to_s)
  end

  def purge_old_typelist(type, max_line)
    type_key = key_prefix("type-#{type}")
    
    @redis.llen(type_key).callback do |len|
      @redis.ltrim(type_key, len - max_line, len) if len > max_line
    end        
  end

  def announce_to_typelist(event)
    typelist_key = key_prefix("type-#{event[:_type]}")
    @redis.lpush(typelist_key, event[:_eid])
  end


  def key_prefix(append=nil)
    [@opts[:redis_prefix], @key, append].compact.join("-")
  end

  def token
    @key
  end
  
  def title
    @title
  end
  
  def active_users_available
    @active_users_available
  end
  
  def dashboards(name=nil)
    return @dashboards unless name
    dash = FnordMetric::Dashboard.new(:title => name)
    @dashboards[dash.token.to_s] ||= dash
  end

  def sessions(_ids, opts={})
    return FnordMetric::Session.all(extend_opts(opts)) if _ids == :all
  end

  def events(_ids, opts={})
    return FnordMetric::Event.all(extend_opts(opts)) if _ids == :all
    return FnordMetric::Event.by_type(opts.delete(:type), extend_opts(opts)) if _ids == :by_type
    return FnordMetric::Event.by_session_key(opts.delete(:session_key), extend_opts(opts)) if _ids == :by_session_key
  end

  def method_missing(m, *args, &block)
    raise "unknown option: #{m}" unless @@opts.include?(m)
    send(:"opt_#{m}", *args, &block)
  end

  def hide_active_users
    @active_users_available = false
  end
  
  def set_title(key)
    @title = key
  end
  
  def opt_event(event_type, opts={}, &block)    
    opts.merge!(:redis => @redis, :gauges => @gauges)   
    FnordMetric::Context.new(opts, block).tap do |context|
      @handlers[event_type.to_s] ||= []
      @handlers[event_type.to_s] << context
    end      
  end

  def opt_gauge(gauge_key, opts={})
    opts.merge!(:key => gauge_key, :key_prefix => key_prefix)
    @gauges[gauge_key] ||= FnordMetric::Gauge.new(opts)   
  end

  def opt_widget(dashboard, widget)
    widget = build_widget(widget) if widget.is_a?(Hash)
    dashboards(dashboard).add_widget(widget)
  end

  def build_widget(opts)
    _gauges = [opts[:gauges]].flatten.map{ |g| @gauges.fetch(g) }
    widget_klass = "FnordMetric::#{opts.fetch(:type).to_s.capitalize}Widget"
    widget_klass.constantize.new(opts.merge(:gauges => _gauges))
  end

  def extend_opts(opts)
    opts.merge(
      :namespace_prefix => key_prefix,
      :redis_prefix => @opts[:redis_prefix],
      :redis => @redis
    )
  end

end
