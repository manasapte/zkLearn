module ZkRecipes 
  class Client
    class Znode
      include MonitorMixin
      attr_reader :name, :parent, :data, :children
      private_class_method :new
      attr_accessor :event_handler

      def initialize(zk, parent, name)
        @zk        = zk
        @parent    = parent
        @name      = name
        @children  = {}
        mon_initialize
      end

      def self.watch(zk, parent, name, &handler)
        new(zk, parent, name).tap do |znode|
          znode.instance_exec(data) do |data|
            event_handler = handler
            subscribe
            @zk.stat(path, :watch => true)
            @zk.children(path, :watch => true)
          end
        end
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

      def path
        if @parent.nil?
          "/"
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
        @event_handler = @zk.register(path) do |event|
          if event.node_deleted?
            post_delete_hook 
          elsif event.node_changed?
            # TODO: implement the hook
          elsif event.node_child?
            sync_children(@zk.children(path, :watch => true))
          end
          event_handler(event)
        end
      end

      def unsubscribe
        @event_handler.unsubscribe if @event_handler
      end
    end
  end
end
