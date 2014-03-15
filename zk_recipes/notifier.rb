module ZkRecipes
  module LeaderElection
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
  end
end
