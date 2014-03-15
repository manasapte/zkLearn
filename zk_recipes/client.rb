module ZkRecipes 
  class Client
    class Znode
      include MonitorMixin
      attr_reader :name, :parent, :data, :children
      private_class_method :new
      attr_accessor :on_change, :on_children, :on_delete

      def initialize(zk, parent, name)
        @zk        = zk
        @parent    = parent
        @name      = name
        @children  = {}
        @created   = false
        @deleted   = false
        mon_initialize
      end

      def self.create_or_get(zk, parent, name, data = nil)
        new(zk, parent, name).tap do |znode|
          znode.instance_exec(data) do |data|
            subscribe
            if @zk.exists?(path, :watch => true)
              raise 'node already exists' unless data.nil
            else
              data = '' if data.nil?
              @zk.create(path, :data => data)
            end
            parent.add_child(self) unless root?
            @zk.children(path, :watch => true)
          end
        end
      end

      def deleted?
        @deleted
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
        synchronize do
          unsubscribe
          @delete = true
        end
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
        post_delete_hook
      rescue ZK::Exceptions::NoNode
      end

      private

      def subscribe
        @event_handler = @zk.register(path) do |event|
          if event.node_deleted?
            post_delete_hook 
            on_delete(event)
          elsif event.node_changed?
            # TODO: implement the hook
            on_change(event)
          elsif event.node_child?
            sync_children(@zk.children(path, :watch => true))
            on_children(event)
          end
        end
      end

      def unsubscribe
        @event_handler.unsubscribe if @event_handler
      end
    end
  end
end
