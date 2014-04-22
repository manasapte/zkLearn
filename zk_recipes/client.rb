module ZkRecipes 
  class Client
    class Zookeeper
      def initialize(zk, notifier, &expiration_handler=nil)
        @znodes            = {}
        @zk                = zk
        @notifier          = notifier
        @exiration_handler = expiration_handler
        @ready             = false
      end

      def manage_connection
        return if @ready
        if e.connecting?
          @notifier.zookeeper_client_connecting
        elsif e.associating?
          @notifier.zookeeper_client_associating
        elsif e.connected?
          @notifier.zookeeper_client_connected
          rewatch_znodes 
        elsif e.auth_failed?
          @notifier.zookeeper_client_auth_failed
        elsif e.expired_session?
          @notifier.zookeeper_client_expired_session
          expiration_handler.call(e)
        else
          @notifier.zookeeper_state_change(e)
        end
      end

      def root(&handler)
        @znodes[Znode::ROOT_PATH] ||= Znode.new(@zk, nil, nil)
      end

      def child(parent, name)
        parent.children[name] || Znode.new(@zk, parent, name).tap { |newnode| @znodes[newnode.path] = newnode } 
      end

      def get(path)
        @znodes[path]
      end

      def rewatch_znodes
        @znodes.each do |znode|
          if znode.watching_children?
            znode.sync_children@zk.children(znode.path, :watch => true)
          end
          if znode.watching_node?
            @zk.stat(znode.path, :watch => true)
          end
        end
      end

    end

    class Znode
      include MonitorMixin
      attr_reader :name, :parent, :data, :children_handlers, :node_handlers
      ROOT_PATH = "/"

      def initialize(zk, parent, name)
        @zk                = zk
        @parent            = parent
        @name              = name
        @children_handlers = []
        @node_handlers     = []
        @client_watch      = false
        mon_initialize
      end

      def watch(&handler)
        subscribe
        @node_handlers << handler
        @zk.stat(path, :watch => true)
      end

      def watch_children(&handler = nil)
        subscribe
        @children_handlers << handler
      end

      def watching_children?
        true
      end

      def watching_node?
        @node_handlers.empty?
      end

      def create(data, mode)
        @zk.create(path, :data => data, :mode => mode)
        parent.add_child(self) unless root?
      end

      def exists?
        @zk.exists?(path)
      end

      def root?
        @parent.nil?
      end

      def add_child(child)
        synchronize do
          @children[child.name] = child
        end
      end

      def data=(data)
        synchronize do
          @zk.set(path, data)
        end
      end

      def data
        @zk.get(path, :watch => true)[0]
      end

      def children
        @children || sync_children(@zk.children(path, :watch => true))
      end

      def path
        if @parent.nil?
          self.class.ROOT_PATH
        else
          File.join([@parent.path, name])
        end
      end

      def post_delete_hook
        unsubscribe
      end

      def sync_children(children)
        synchronize do
          children.each do |newchild|
            unless @children.has_key(newchild)
              self.class.new(@zk, self, newchild)
            end
          end
          (@children.keys - children).each do |dead_child|
            @children.delete(dead_child)
          end
        end
      end

      def delete
        synchronize do
          @children.each(&:delete) 
          @zk.delete(path)
        end
      rescue ZK::Exceptions::NoNode
        false
      else
        true
      end

      private

      def subscribe
        return if @event_handler
        @event_handler = @zk.register(path) do |event|
          if event.node_created? || event.node_deleted? || event.node_changed?
            node_handlers.delete_if { |h| true }.each { |h| h.call(event) }
          elsif event.node_child?
            sync_children(@zk.children(path, :watch => true))
            children_handlers.delete_if { |h| true }.each { |ch| ch.call(event) }
          end
        end
      end

      def unsubscribe
        if @event_handler
          @event_handler.unsubscribe
          remote_instance_variable(:@event_handler)
        end
      end
    end
  end
end
