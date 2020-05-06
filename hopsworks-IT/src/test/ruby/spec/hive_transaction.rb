=begin
 This file is part of Hopsworks
 Copyright (C) 2020, Logical Clocks AB. All rights reserved

 Hopsworks is free software: you can redistribute it and/or modify it under the terms of
 the GNU Affero General Public License as published by the Free Software Foundation,
 either version 3 of the License, or (at your option) any later version.

 Hopsworks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 PURPOSE.  See the GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/>.
=end
require 'pp'
describe "On #{ENV['OS']}" do
  describe 'transactions' do
    def wait_for_me_time(timeout=480)
      start = Time.now
      x = yield
      until x["success"] == true
        if Time.now - start > timeout
          break
        end
        sleep(1)
        x = yield
      end
      return x
    end
    
    def epipe_stop
      execute_remotely ENV['EPIPE_HOST'], "sudo systemctl stop epipe"
    end
    def epipe_restart
      execute_remotely ENV['EPIPE_HOST'], "sudo systemctl restart epipe"
    end

    def epipe_active
      output = execute_remotely ENV['EPIPE_HOST'], "systemctl is-active epipe"
      expect(output.strip).to eq("active"), "epipe is down"
    end

    def is_epipe_active
      output = execute_remotely ENV['EPIPE_HOST'], "systemctl is-active epipe"
      output.strip.eql? "active"
    end
    def epipe_wait_on_mutations(repeat=3)
      epipe_active
      repeat.times do
        result = wait_for_me_time(30) do
          pending_mutations = HDFSMetadataLog.count
          if pending_mutations == 0
            { 'success' => true }
          else
            { 'success' => false, 'msg' => "hdfs_metadata_logs is not being consumed by epipe - pending:#{pending_mutations}" }
          end
        end
        if result["success"] == true
          break
        else
          pp "WARNING - #{result["msg"]}"
          epipe_restart
          sleep(1)
          epipe_active
        end
      end
      pending_mutations = HDFSMetadataLog.count
      expect(pending_mutations).to eq(0), "hdfs_metadata_logs is not being consumed by epipe - pending:#{pending_mutations}"
      #wait for epipe-elasticsearch propagation
      sleep(3)
    end

    def epipe_wait_on_provenance(repeat=3)
      epipe_active
      repeat.times do
        #check FileProv log table
        result = wait_for_me_time(30) do
          pending_prov = FileProv.count
          if pending_prov == 0
            { 'success' => true }
          else
            { 'success' => false, 'msg' => "hdfs_file_prov_logs is not being consumed by epipe - pending:#{pending_prov}" }
          end
        end
        if result["success"] == true
          #check AppProv log table
          result = wait_for_me_time(30) do
            pending_prov = AppProv.count
            if pending_prov == 0
              { 'success' => true }
            else
              { 'success' => false, 'msg' => "hdfs_app_prov_logs is not being consumed by epipe - pending:#{pending_prov}" }
            end
          end
        end
        if result["success"] == true
          break
        else
          #restart epipe and try waiting again
          pp "WARNING - #{result["msg"]}"
          epipe_restart
          sleep(1)
          epipe_active
        end
      end
      pending_prov = FileProv.count
      expect(pending_prov).to eq(0), "hdfs_file_prov_logs is not being consumed by epipe - pending:#{pending_prov}"
      pending_prov = AppProv.count
      expect(pending_prov).to eq(0), "hdfs_app_prov_logs is not being consumed by epipe - pending:#{pending_prov}"
      #wait for epipe-elasticsearch propagation
      sleep(3)
    end
    def login_user(email, password)
      reset_session
      response = post "#{ENV['HOPSWORKS_API']}/auth/login", URI.encode_www_form({ email: email, password: password}), {content_type: 'application/x-www-form-urlencoded'}
      return response, headers
    end

    def del_featuregroup(user, project_id,  featurestore_id, fg_id)
      response, headers = login_user(user, "Pass123")
      delete_featuregroup_endpoint = "#{ENV['HOPSWORKS_API']}/project/#{project_id}/featurestores/#{featurestore_id}/featuregroups/#{fg_id}"
      if !headers["set_cookie"].nil? && !headers["set_cookie"][1].nil?
        cookie = headers["set_cookie"][1].split(';')[0].split('=')
        cookies = {"SESSIONID"=> JSON.parse(response.body)["sessionID"], cookie[0] => cookie[1]}
      else
        cookies = {"SESSIONID"=> JSON.parse(response.body)["sessionID"]}
      end
      request = Typhoeus::Request.new(
          "https://#{ENV['WEB_HOST']}:#{ENV['WEB_PORT']}#{delete_featuregroup_endpoint}",
          headers: {:cookies => cookies, 'Authorization' => headers["authorization"]},
          method: "delete",
          followlocation: true,
          ssl_verifypeer: false,
          ssl_verifyhost: 0)

      request.on_complete do |response|
        if response.success?
          pp "Delete featuregroup response: " + response.code.to_s
        elsif response.timed_out?
          pp "Timed out deleting featuregroup"
        elsif response.code == 0
          pp response.return_message
        else
          pp "Delete featuregroup - HTTP request failed: " + response.code.to_s
        end
      end
      return request
    end

    def cre_featuregroup(user, project_id,  featurestore_id, fg_name, q)
      response, headers = login_user(user, "Pass123")
      create_featuregroup_endpoint = "#{ENV['HOPSWORKS_API']}/project/#{project_id}/featurestores/#{featurestore_id}/featuregroups"
      pp "create #{fg_name}"
      if !headers["set_cookie"].nil? && !headers["set_cookie"][1].nil?
        cookie = headers["set_cookie"][1].split(';')[0].split('=')
        cookies = {"SESSIONID"=> JSON.parse(response.body)["sessionID"], cookie[0] => cookie[1]}
      else
        cookies = {"SESSIONID"=> JSON.parse(response.body)["sessionID"]}
      end
      features = []
      100.times do |i|
        features[i] = {
            type: "INT",
            name: "#{fg_name}_#{i}",
            description: "testfeaturedescription",
            primary: false
        }
      end
      features[0][:primary] = true
      json_data = {
          name: fg_name,
          jobs: [],
          features: features,
          description: "fg description",
          version: 1,
          type: "cachedFeaturegroupDTO",
          onlineFeaturegroupDTO: nil,
          featuregroupType: "CACHED_FEATURE_GROUP",
          onlineFeaturegroupEnabled: false
      }
      json_data = json_data.to_json

      request = Typhoeus::Request.new(
          "https://#{ENV['WEB_HOST']}:#{ENV['WEB_PORT']}#{create_featuregroup_endpoint}",
          headers: {:cookies => cookies, 'Authorization' => headers["authorization"], 'Content-Type' => "application/json"},
          method: "post",
          body: json_data,
          followlocation: true,
          ssl_verifypeer: false,
          ssl_verifyhost: 0)

      request.on_complete do |response|
        if response.success?
          pp "Create featuregroup response: " + response.code.to_s
          # pp "#{response.body}"
          q << JSON.parse(response.body)["id"]
        elsif response.timed_out?
          pp "Timed out creating featuregroup"
        elsif response.code == 0
          pp response.return_message
        else
          pp "Create featuregroup:#{fg_name} - HTTP request failed: " + response.code.to_s
        end
      end
      return request
    end

    it 'add hive 10000 negs' do
      10000.times do |i|
        HiveCDS.create(CD_ID: -1 * i)
      end
    end

    it 'count' do
      pp NDBTransactions.count
      project1 = create_project
      user_email = @user[:email]
      pp "#{user_email}"
      pp NDBTransactions.count
      featurestore_id = get_featurestore_id(project1[:id])

      fgs_nr = 50
      q = Queue.new
      pp "create"
      hydra = Typhoeus::Hydra.new(max_concurrency: 10)
      fgs_nr.times do |i|
        hydra.queue cre_featuregroup(user_email, project1[:id], featurestore_id, "fg_a#{i}", q)
      end
      hydra.run

      pp "mix"
      hydra = Typhoeus::Hydra.new(max_concurrency: 1)
      fgs_nr.times do |i|
        hydra.queue cre_featuregroup(user_email, project1[:id], featurestore_id, "fg_b#{i}", q)
        hydra.queue del_featuregroup(user_email, project1[:id], featurestore_id, q.pop)
      end
      hydra.run

      pp "deleting"
      pp NDBTransactions.count
      hydra = Typhoeus::Hydra.new(max_concurrency: 10)
      q.size.times do
        hydra.queue del_featuregroup(user_email, project1[:id], featurestore_id, q.pop)
      end
      hydra.run
    end

    it 'cleanup' do
      clean_all_test_projects
    end
  end
end
