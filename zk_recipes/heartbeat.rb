module ZkRecipes
  class Heartbeat
    def initialize(zk, namespace, period)
      @namespace = namespace.gsub!('/^\//', '')
      unless @zk.stat(@namespace).exists
        @zk.create(@namespace)
      end
      @period = period
    end

    def start
      @t = Thread.new { heartbeat }
    end

    def join
      @t.join
    end

    def stop
      @t.kill
    end

    private

    def heartbeat
      @zk.stat(@namespace, :watch => false)
    rescue =>e
    else
      sleep @period
    end
  end
end
