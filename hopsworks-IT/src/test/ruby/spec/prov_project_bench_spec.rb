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
    with_valid_session
    pp "user email: #{@user["email"]}"
    @project1_name = "prov_proj_#{short_random_id}"
    pp "create project: #{@project1_name}"
    @project1 = create_project_by_name(@project1_name)
    @app1_id = "application_#{short_random_id}_0001"
  end

  # after :all do 
  #   pp "delete projects"
  #   delete_project(@project1)
  # end

  def t_request_success(request, metrics)
    request.on_complete do |response|
      if response.success?
        #pp "success"
        #pp response.total_time
        metrics << response.total_time
      elsif response.timed_out?
        pp "got a time out"
      elsif response.code == 0
        # Could not get an http response, something's wrong.
        pp response.return_message
      else
        # Received a non-successful http response.
        pp "#{response.body}" 
        pp "HTTP request failed: " + response.code.to_s + " - " + response.return_message
      end
      expect(response.code).to eq 200
    end
  end

  def t_request_followup(hydra, parent, child, metrics)
    parent.on_complete do |response|
      if response.success?
        #pp "success f"
        #pp response.total_time
        metrics << response.total_time
        hydra.queue(child)
      elsif response.timed_out?
        pp "got a time out"
      elsif response.code == 0
        # Could not get an http response, something's wrong.
        pp response.return_message
      else
        # Received a non-successful http response.
        pp "#{response.body}" 
        pp "HTTP request failed: " + response.code.to_s + " - " + response.return_message
      end
      expect(response.code).to eq 200
    end
  end

  def t_request_umb(url, method, body) 
    Typhoeus::Request.new(
      url, 
      headers: {:cookies => @cookies, 'Content-Type' => 'application/json', 'Authorization' => @token},
      body: body,
      method: method,
      followlocation: true,
      ssl_verifypeer: false,
      ssl_verifyhost: 0)
  end

  def t_request_um(url, method) 
    Typhoeus::Request.new(
      url, 
      headers: {:cookies => @cookies, 'Content-Type' => 'application/json', 'Authorization' => @token},
      method: method,
      followlocation: true,
      ssl_verifypeer: false,
      ssl_verifyhost: 0)
  end

  def t_create_file(project, path, metrics) 
    resource = "project/#{project[:id]}/dataset"
    url = "https://#{ENV['WEB_HOST']}:#{ENV['WEB_PORT']}#{ENV['HOPSWORKS_API']}/#{resource}"
    #pp url
    body = JSON.parse('{"name": "' + "#{path}" + '"}').to_json
    #pp body
    #puts body
    request = t_request_umb(url, :post, body)
    t_request_success(request, metrics)
    request
  end

  def t_delete_file(project, path, metrics) 
    resource = "project/#{project[:id]}/dataset/file/#{path}"
    url = "#{ENV['HOPSWORKS_API']}/#{resource}"
    #pp url
    delete target
    request = t_request_um(url, :delete)
    t_request_success(request, metrics)
    request
  end

  def t_create_delete_file(hydra, project, path, metrics)
    c_resource = "project/#{project[:id]}/dataset"
    c_url = "https://#{ENV['WEB_HOST']}:#{ENV['WEB_PORT']}#{ENV['HOPSWORKS_API']}/#{c_resource}"
    #pp c_url
    c_body = JSON.parse('{"name": "' + "#{path}" + '"}').to_json
    #pp c_body
    #puts c_body
    c_request = t_request_umb(c_url, :post, c_body)

    d_resource = "project/#{project[:id]}/dataset/file/#{path}"
    d_url = "https://#{ENV['WEB_HOST']}:#{ENV['WEB_PORT']}#{ENV['HOPSWORKS_API']}/#{d_resource}"
    #pp d_url
    d_request = t_request_um(d_url, :delete)
    t_request_success(d_request, metrics)

    t_request_followup(hydra, c_request, d_request, metrics)
    c_request
  end

  def t_create_experiment(project, experiment_id, metrics) 
    t_create_file(project, "Experiments/#{experiment_id}", metrics)
  end

  def t_experiment_state_1(project, metrics) 
    resource = "project/#{project[:id]}/provenance/file/state"
    query_params = "filter_by=ML_TYPE:EXPERIMENT&sort_by=CREATE_TIMESTAMP:desc&offset=0&limit=1000"
    url = "https://#{ENV['WEB_HOST']}:#{ENV['WEB_PORT']}#{ENV['HOPSWORKS_API']}/#{resource}?#{query_params}"
    pp url
    request = t_request_um(url, :get)
    t_request_success(request, metrics)
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

  describe 'one size - benchmark ops' do
    describe 'ml asset (experiment) state' do
      it "epipe check" do
        execute_remotely @hostname, "sudo systemctl restart epipe"
      end
      it "create experiments" do
        request = t_create_experiment(@project1, "#{@app1_id}_1")
        request.run
      end
      it "epipe wait" do 
        prov_wait_for_epipe() 
      end
      it "benchmark" do
        Benchmark.bm do |x|
          x.report {
            request = t_experiment_state_1(@project1)
            request.run
            pp JSON.parse(request.response.body)["result"].length
          }
        end
      end
    end
  end

  def experiment_batch(hydra, batchNr) 
    batchE = 10
    pp "experiment batch #{batchNr} (#{batchE})"
    metrics = Concurrent::Array.new()     
    for i in 0..(batchE-1) do
      exp_id = batchNr*batchE + i
      experiment_create = t_create_file(@project1, "Experiments/#{@app1_id}_#{exp_id}", metrics)
      hydra.queue(experiment_create) 
    end
    hydra.run 
    pp metrics.percentile(95)
  end

  def experiment_ops_batch(hydra, batchNr)
    batchE = 10
    batchO = 2
    pp "experiment ops batch #{batchNr} (#{batchE} #{batchO})"
    metrics = Concurrent::Array.new()
    for i in 0..(batchE-1) do
      for j in 1..batchO do  
        exp_id = batchNr*batchE+i
        file_create = t_create_delete_file(hydra, @project1, "Experiments/#{@app1_id}_#{exp_id}/file_#{j}", metrics)
        hydra.queue(file_create)
      end
    end
    hydra.run 
    pp metrics.percentile(95)
  end

  describe 'loaded index - benchmark ops' do
    describe 'ml asset (experiment) state' do
      it "epipe check"  do
        execute_remotely @hostname, "sudo systemctl restart epipe"
      end
      it "create experiments (1000)" do
        hydra = Typhoeus::Hydra.new(max_concurrency: 20)
        for i in 0..3 do
          experiment_batch(hydra, i)
        end
        for i in 0..3 do
          experiment_ops_batch(hydra, i)
        end
      end
      it "epipe wait" do 
        prov_wait_for_epipe() 
      end
      it "benchmark" do
        Benchmark.bm do |x|
          metrics = Concurrent::Array.new()   
          x.report {
            request = t_experiment_state_1(@project1, metrics)
            request.run
            #pp request.response.body
            pp JSON.parse(request.response.body)["items"].length
          }
          x.report {
            request = t_experiment_state_1(@project1, metrics)
            request.run
            #pp request.response.body
            pp JSON.parse(request.response.body)["items"].length
          }
          pp metrics.percentile(95)
        end
      end
    end
  end
end
