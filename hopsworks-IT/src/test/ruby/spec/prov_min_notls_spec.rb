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

describe "On #{ENV['OS']}" do
  before :all do
    $stdout.sync = true
    with_valid_session
    pp "user email: #{@user["email"]}"
    check_archive_disabled()
    @project1_name = "prov_proj_#{short_random_id}"
    @project2_name = "prov_proj_#{short_random_id}"
    @app1_id = "application_#{short_random_id}_0001"
    @app2_id = "application_#{short_random_id}_0001"
    @app3_id = "application_#{short_random_id}_0001"
    @experiment_app1_name1 = "#{@app1_id}_1"
    @experiment_app2_name1 = "#{@app2_id}_1"
    @experiment_app3_name1 = "#{@app3_id}_1"
    @experiment_app1_name2 = "#{@app1_id}_2"
    @experiment_app1_name3 = "#{@app1_id}_3"
    @app_file_history1   = "application_#{short_random_id}_fh1"
    @app_file_history2   = "application_#{short_random_id}_fh2"
    @app_file_history3   = "application_#{short_random_id}_fh3"
    @app_file_ops1      = "application_#{short_random_id}_fo1"
    @app_file_ops2      = "application_#{short_random_id}_fo2"
    @app_app_fprint     = "application_#{short_random_id}_af"
    @experiment_file_history    = "#{@app_file_history1}_1"
    @experiment_file_ops        = "#{@app_file_ops1}_1"
    @experiment_app_fprint      = "#{@app_app_fprint}_1"
    @not_experiment_name = "not_experiment"
    @model1_name = "model_a"
    @model2_name = "model_b"
    @model_version1 = "1"
    @model_version2 = "2"
    @td1_name = "td_a"
    @td2_name = "td_b"
    @td_version1 = "1"
    @td_version2 = "2"
    @xattrV1 = JSON['{"f1_1":"v1","f1_2":{"f2_1":"val1"}}']
    @xattrV2 = JSON['{"f1_1":"v1","f1_2":{"f2_2":"val2"}}']
    @xattrV3 = JSON['[{"f3_1":"val1","f3_2":"val2"},{"f4_1":"val3","f4_2":"val4"}]']
    @xattrV4 = "notJson"
    @xattrV5 = JSON['[{"f3_1":"val1","f3_2":"val1"},{"f3_1":"val2","f3_2":"val2"}]']
    @xattrV6 = "notJava"
    @xattrV7 = "not Json"
    @xattrV8 = JSON['{"name": "fashion mnist gridsearch"}']
  end

  def fix_json(json)
    json1 = json.gsub(/([a-zA-Z_1-9]+)=/, '"\1"=')
    json2 = json1.gsub(/=([a-zA-Z_1-9]+)/, '="\1"')
    json3 = json2.gsub('=', ':')
    json4 = JSON.parse(json3)
  end

  describe 'test suite - 1 project' do
    before :all do
      pp "create project: #{@project1_name}"
      @project1 = create_project_by_name(@project1_name)
    end

    before :each do
      prov_wait_for_epipe()
    end
    after :each do
      prov_wait_for_epipe()
      pp "cleanup cycle"
      ops = cleanup_cycle(@project1)

      #pp ops
      if ops["count"] != 0
        pp "secondary cleanup cycle"
        sleep(1)
        ops = cleanup_cycle(@project1)
      end
      expect(ops["count"]).to eq 0

      prov_wait_for_epipe()
    end

    after :all do
      pp "delete projects"
      delete_project(@project1)
      @project1 = nil
    end

    describe 'experiments' do
      it 'experiment - app expansion - no app id' do
        pp "stop epipe"
        execute_remotely @hostname, "sudo systemctl stop epipe"

        pp "setup"
        project = @project1
        #no app id
        prov_create_experiment(project, @experiment_app1_name1)
        #app id attached as xattr
        prov_create_experiment(project, @experiment_app2_name1)
        experiment2_record = prov_get_experiment_record(project, @experiment_app2_name1)
        expect(experiment2_record.length).to eq 1
#         pp experiment2_record[0]
        prov_add_xattr(experiment2_record[0], "app_id", @app2_id, "XATTR_ADD", 1)
        #setup fake app
        user_name = experiment2_record[0]["io_user_name"]
        prov_add_app_states2(@app2_id, user_name)
        #app id (tls) created by app
        prov_create_experiment(project, @experiment_app3_name1)
        experiment3_record = prov_get_experiment_record(project, @experiment_app3_name1)
        expect(experiment3_record.length).to eq 1
        experiment3_record[0]["io_app_id"] = @app3_id
#         pp experiment3_record[0]
        experiment3_record[0].save!
        #setup fake app
        user_name = experiment3_record[0]["io_user_name"]
        prov_add_app_states2(@app3_id, user_name)

        pp "restart epipe"
        execute_remotely @hostname, "sudo systemctl restart epipe"
        prov_wait_for_epipe()

        pp "test query"
        query = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/state?filter_by=ML_TYPE:EXPERIMENT&expand=APP"
        pp "#{query}"
        result = get "#{query}"
        expect_status(200)
        parsed_result = JSON.parse(result)
        expect(parsed_result["count"]).to eq 3
#         pp parsed_result
        prov_check_experiment3(parsed_result["items"], @experiment_app1_name1, "UNKNOWN")
        prov_check_experiment3(parsed_result["items"], @experiment_app2_name1, "FINISHED")
        prov_check_experiment3(parsed_result["items"], @experiment_app3_name1, "FINISHED")

        pp "cleanup hops"
        prov_delete_experiment(@project1, @experiment_app1_name1)
        prov_delete_experiment(@project1, @experiment_app2_name1)
        prov_delete_experiment(@project1, @experiment_app3_name1)

        pp "check hops cleanup"
        prov_wait_for_epipe()
        get_ml_asset_in_project(@project1, "EXPERIMENT", false, 0)
      end
    end
  end
end