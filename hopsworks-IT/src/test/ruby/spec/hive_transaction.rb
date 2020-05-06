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
      # pp "create #{fg_name}"
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

      fgs_nr = 10
      q = Queue.new
      pp "create"
      hydra = Typhoeus::Hydra.new(max_concurrency: 10)
      fgs_nr.times do |i|
        hydra.queue cre_featuregroup(user_email, project1[:id], featurestore_id, "fg_a#{i}", q)
      end
      hydra.run

      pp "mix"
      hydra = Typhoeus::Hydra.new(max_concurrency: 5)
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
