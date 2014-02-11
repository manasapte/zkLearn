module ZkRecipes
  class Heartbeat
    def initialize(zk, period)
      @zk = zk
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
      loop do
        begin
          @zk.stat("/", :watch => false)
        rescue =>e
          p "caught exception #{e.inspect}"
          exit 2
        else
          sleep @period
        end
      end
    end
  end
end
