module ZkRecipes
  module Election
    require 'heartbeat'
    HEARTBEAT_PERIOD = 2
    class ElectionCandidate
      def initialize(zk, app_name, election_ns, handler)
        @zk          = zk
        @namespace   = app_name
        @election_ns = election_ns
        @handler     = handler
        @prefix      = get_path([@namespace, @election_ns, "_vote_"]))
        @path        = nil
        @heartbeat   = ZkRecipes::Heartbeat.new(@zk, @namespace, HEARTBEAT_PERIOD)
      end

      def get_path(crumbs)
        File.join([""]+crumbs)
      end

      def leader_path
        


      def start
        @zk.register(leader_path) do |event|
          if event.node_deleted?
            am_i_the_new_leader
        end
        @heartbeat.start
        @path = @zk.create(:mode => :ephemeral_sequential)


