module ZkRecipes 
  class Notifier
    def initialize(io)
      @io = io
    end

    def service_create_attempt(type, new_service)
      @io << "for type: #{type} trying to create: #{new_service.metadata['sid']}\n"
    end

    def service_delete_attempt(type, dead_service)
      @io << "for type: #{type} trying to delete dead service: #{dead_service}\n"
    end

    def services_received(type, services)
      @io << "for type: #{type} received #{services.length} services: #{services.map {|s| s.metadata['sid']}.join(",")}\n"
    end
  end

  class Datastore 
    def initialize(zk, node_class)
      @zk         = zk
      @node_class = node_class
    end

    def root
      @root ||= @node_class.new(@zk, nil, '').tap do |r|
        r.create_if_not_exists
      end
    end

    def region(r_name)
      region = root.children[r_name]
      region || @node_class.new(@zk, root, r_name).tap do |node|
        node.create_if_not_exists
      end
    end

    def type(r_name, t_name)
      rgn  = region(r_name)
      type = rgn.children[t_name]
      type || @node_class.new(@zk, rgn, t_name).tap do |node|
        node.create_if_not_exists
      end
    end

    def service(r_name, t_name, s_name, data=nil)
      tp   = type(r_name, t_name)
      serv = tp.children[s_name]
      serv || @node_class.new(@zk, tp, s_name).tap do |node|
        node.create_if_not_exists(data, true)
      end
    end
  end

  class Znode
    include MonitorMixin
    attr_reader :name, :parent, :data, :children
    def initialize(zk, parent, name)
      @zk        = zk
      @parent    = parent
      @name      = name
      @children  = {}
      @created   = false
      @deleted   = false
      mon_initialize
    end

    def deleted?
      @deleted
    end

    def create_if_not_exists(data = nil, force = false)
      subscribe
      if @zk.exists?(path, :watch => true)
        self.data = data if force
      else
        data = '' if data.nil?
        @zk.create(path, :data => data)
      end
      @parent.add_child(self) unless root?
      @zk.children(path, :watch => true)
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
        File.join(["", ""])
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
        elsif event.node_changed?
          # TODO: implement the hook
        elsif event.node_child?
          sync_children(@zk.children(path, :watch => true))
        end
      end
    end

    def unsubscribe
      @event_handler.unsubscribe if @event_handler
    end
  end
end
