module ServiceDiscovery
  module Collectors
    class ZookeeperCollector
      class Znode
        include MonitorMixin
        attr_reader :name, :parent, :data, :children_handlers, :node_handlers

        def initialize(zk, parent, name, notifier)
          @zk                = zk
          @parent            = parent
          @name              = name
          @notifier          = notifier
          @children_handlers = []
          @node_handlers     = []
          @client_watch      = false
          mon_initialize
        end

        def watch(&handler)
          subscribe
          @node_handlers << handler if !handler.nil?
        end

        def watch_children(&handler)
          subscribe
          @children_handlers << handler if !handler.nil?
        end

        def watching_children?
          @children_handlers.empty?
        end

        def watching_node?
          @node_handlers.empty?
        end

        def each_descendent_bottomup(&block)
          children.values.each {|ch| ch.each_descendent_bottomup(&block)}
          yield(self)
          self
        end

        def each_descendent_topdown(&block)
          yield(self)
          children.values.each {|ch| ch.each_descendent_topdown(&block)}
          self
        end


        def create(data, mode=nil)
          $stderr.puts("trying to create #{path} with data #{data}")
          @zk.create(path, :data => data, :mode => mode)
          parent.add_child(self) unless root?
        end

        def exists?(watch = true)
          @zk.exists?(path, :watch => watch)
        end

        def stat(watch = true)
          @zk.stat(path, :watch => watch)
        end

        def root?
          @parent.nil?
        end

        def children?
          !!@children
        end

        def add_child(child)
          synchronize do
            #$stderr.puts("in add child for #{path} and child name: #{child.name}")
            @children[child.name] = child
          end
        end

        def remove_child(child)
          synchronize do
            #$stderr.puts("in add child for #{path} and child name: #{child.name}")
            @children.delete(child.name)
          end
        end

        def data=(data)
          synchronize do
            @zk.set(path, data)
          end
        end

        def data(watch = true)
          @zk.get(path, :watch => watch).first
        end

        def children
          #$stderr.puts("in children for #{self.object_id} and children: #{@children}")
          @children ||= {}
        end

        def path
          if @parent.nil?
            '/'
          else
            File.join([@parent.path, name])
          end
        end

        def invalidate_children
          synchronize { remove_instance_variable(:@children) }
        end

        def sync_children(&block)
          #$stderr.puts("in sync children for #{path} and children: #{children.inspect}")
          new_children = []
          synchronize do
            zk_children = @zk.children(path, :watch=>true)
            zk_children.each do |cname|
              unless children.has_key?(cname)
                $stderr.puts("trying to create #{cname} for #{path}")
                self.class.new(@zk, self, cname, @notifier).tap do |newnode|
                  add_child(newnode)
                  new_children << newnode
                end
              end
            end
            (children.keys - zk_children).each do |dead_child|
              @children.delete(dead_child)
            end
          end
          yield(new_children) if block_given?
        end

        def delete
          synchronize do
            children.values.each(&:delete) 
            @zk.delete(path)
            parent.remove_child(self) unless root?
          end
        rescue ZK::Exceptions::NoNode
          parent.remove_child(self)
          false
        else
          true
        end

        def unsubscribe
          if @event_handler
            @event_handler.unsubscribe
            remove_instance_variable(:@event_handler)
          end
        end

        private

        def subscribe
          return if @event_handler
          @event_handler = @zk.register(path) do |event|
            begin
              if event.node_created? || event.node_deleted? || event.node_changed?
                node_handlers.each { |h| h.call(event, data) }
                node_handlers.clear
              elsif event.node_child?
                children_handlers.each { |ch| ch.call(event, children) }
                children_handlers.clear
              end
            rescue ::Zookeeper::Exceptions::NotConnected   
              @notifier.not_connected {"callback thread"}
            rescue => e
              @notifier.failed(e)
            end
          end
        end
      end
    end
  end
end

