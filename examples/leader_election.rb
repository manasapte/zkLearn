require_relative '../zk_recipes/election'
require 'zk'
class LeaderElection 
  def run
    @candidate = ZkRecipes::Election::ElectionCandidate.new(ZK::Client::Threaded.new('localhost:2181'), 'testapp', 'mydata', 'election', self)
    @candidate.start
  end

  def leader_ready!
    p "leader ready"
  end

  def election_won!
    p "won the election"
  end

  def election_lost!
    p "lost the election"
  end
end

can = LeaderElection.new
can.run
