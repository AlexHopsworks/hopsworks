=begin
 This file is part of Hopsworks
 Copyright (C) 2018, Logical Clocks AB. All rights reserved

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
require 'benchmark'
require 'typhoeus'
require 'descriptive_statistics'
require 'concurrent'

describe "On #{ENV['OS']}" do
  before :all do
    $stdout.sync = true
    user_mail="43ef7f5b7ab95f75583729120dd1ef8e19ac4e69@email.com"
    user_pass="Pass123"
    create_session(user_mail, user_pass)
    @t_cookies =''
    @t_token = ''
    Airborne.configure do |config|
      @t_cookies = config.headers[:cookies]
      @t_token = config.headers["Authorization"]
    end
    pp @t_token
    project_name="prov_proj_2f727e3fbb2a9758"
    @project = get_project_by_name(project_name)
  end

  def my_call() 
    resource = "project/#{@project[:id]}/provenance/file/state"
    query_params = "filter_by=ML_TYPE:EXPERIMENT&sort_by=CREATE_TIMESTAMP:desc&offset=0&limit=12"
    url = "https://#{ENV['WEB_HOST']}:#{ENV['WEB_PORT']}#{ENV['HOPSWORKS_API']}/#{resource}?#{query_params}"
    pp url
    request = t_request_um(url, :get)
    # body = '{"name": "' + "Experiments/#{experiment_id}" + '"}'
    # pp body
    # request = t_request_body(url, :post, body)
  end

  def t_request_success(request, metrics)
    request.on_complete do |response|
      if response.success?
        pp "success"
        #pp response.body
        metrics << response.total_time
      elsif response.timed_out?
        # aw hell no
        pp "got a time out"
      elsif response.code == 0
        # Could not get an http response, something's wrong.
        pp response.return_message
      else
        # Received a non-successful http response.
        pp "HTTP request failed: " + response.code.to_s
      end
      #expect(response.code).to eq 200
    end
  end

  def t_request_umb(url, method, body) 
    request = Typhoeus::Request.new(
      url, 
      headers: {:cookies => @t_cookies, 'Content-Type' => 'application/json', 'Authorization' => @t_token},
      body: body,
      method: method,
      followlocation: true,
      ssl_verifypeer: false,
      ssl_verifyhost: 0)
    request
  end

  def t_request_um(url, method) 
    request = Typhoeus::Request.new(
      url, 
      headers: {:cookies => @t_cookies, 'Content-Type' => 'application/json', 'Authorization' => @t_token},
      method: method,
      followlocation: true,
      ssl_verifypeer: false,
      ssl_verifyhost: 0)
    request
  end

  def t_e_request_umb(url, body) 
    Typhoeus::Request.new(
      url, 
      headers: {'Content-Type' => 'application/json'},
      method: :get,
      body: body,
      followlocation: true,
      ssl_verifypeer: false,
      ssl_verifyhost: 0)
  end

  describe 'test' do
    it "epipe wait" do 
      prov_wait_for_epipe() 
    end

    it "test your call" do
      Benchmark.bm do |x|
        x.report {
          request = my_call()
          request.run
          #pp JSON.parse(request.response.body)
        }
      end
    end
  end

  describe 'hopsworks' do
    def t_experiment_state_1(project, metrics) 
      resource = "project/#{project[:id]}/provenance/file/state"
      query_params = "filter_by=ML_TYPE:EXPERIMENT&sort_by=CREATE_TIMESTAMP:desc&offset=0&limit=1000"
      url = "https://#{ENV['WEB_HOST']}:#{ENV['WEB_PORT']}#{ENV['HOPSWORKS_API']}/#{resource}?#{query_params}"
      #pp url
      request = t_request_um(url, :get)
      t_request_success(request, metrics)
      request
    end
    describe 'small' do
      it "benchmark" do
        Benchmark.bm do |x|
          metrics = Concurrent::Array.new()   
          x.report {
            request = t_experiment_state_1(@project, metrics)
            request.run
            #pp request.response.body
            pp JSON.parse(request.response.body)["items"].length
          }
          x.report {
            request = t_experiment_state_1(@project, metrics)
            request.run
            #pp request.response.body
            pp JSON.parse(request.response.body)["items"].length
          }
          pp metrics.percentile(95)
        end
      end
    end

    describe 'concurrent load' , focus: true do
      it "benchmark" do
        metrics = Concurrent::Array.new()   
        hydra = Typhoeus::Hydra.new(max_concurrency: 1100)
        #warm up run
        for i in 1..100 do
          hydra.queue t_experiment_state_1(@project, metrics)
        end 
        hydra.run
        pp "completed: #{metrics.length}"
        pp "50:#{metrics.percentile(50)} 80:#{metrics.percentile(80)} 90:#{metrics.percentile(90)}"
        metrics = Concurrent::Array.new()   
        #
        for i in 0..5000 do
          hydra.queue t_experiment_state_1(@project, metrics)
        end
        hydra.run
        pp "completed: #{metrics.length}"
        pp "50:#{metrics.percentile(50)} 80:#{metrics.percentile(80)} 90:#{metrics.percentile(90)}"
      end
    end
  end
  describe 'raw elastic' do
    def t_elastic_1(project_inode_id, metrics) 
      url = "http://#{ENV['ELASTIC_API']}/fileprovenance/_search"
      body = JSON.parse('{"from":10,"size":20,"query":{"bool":{"must":[{"term":{"entry_type":{"value":"state","boost":1.0}}},{"term":{"mlType":{"value":"EXPERIMENT","boost":1.0}}},{"term":{"project_i_id":{"value":' + "#{project_inode_id}" + ',"boost":1.0}}}],"adjust_pure_negative":true,"boost":1.0}},"sort":[{"create_timestamp":{"order":"desc"}}]}').to_json
      pp "#{url} #{body}"
      request = t_e_request_umb(url, body)
      t_request_success(request, metrics)
      request
    end

    it "benchmark" do
      Benchmark.bm do |x|
        metrics = Concurrent::Array.new()   
        x.report {
          request = t_elastic_1(508092, metrics)
          request.run
          pp JSON.parse(request.response.body)["hits"]["total"]
          pp JSON.parse(request.response.body)["hits"]["hits"].length
        }
        x.report {
          request = t_elastic_1(508092, metrics)
          request.run
          pp JSON.parse(request.response.body)["hits"]["total"]
          pp JSON.parse(request.response.body)["hits"]["hits"].length
        }
        pp metrics.percentile(95)
      end
    end
  end
end