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
    @project3_name = "prov_proj_#{short_random_id}"
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
  
  it "test epipe" do
    pp "restart epipe"
    execute_remotely @hostname, "sudo systemctl restart epipe"

    pp "wait for epipe"
    prov_wait_for_epipe() 
  end

  describe 'test suite - 1 project' do
    before :all do
      pp "create project: #{@project1_name}"
      @project1 = create_project_by_name(@project1_name)
    end
    
    after :each do
      pp "cleanup cycle"
      ops = cleanup_cycle(@project1)

      #pp ops
      if ops["count"] != 0
        pp "secondary cleanup cycle"
        sleep(1)
        ops = cleanup_cycle(@project1)
      end
      expect(ops["count"]).to eq 0
    end

    after :all do
      pp "delete projects"
      delete_project(@project1)
      @project1 = nil
    end

    it 'mock app file operations' do
      pp "stop epipe"
      execute_remotely @hostname, "sudo systemctl stop epipe"

      pp "create mock file operations - file operations"
      prov_create_experiment(@project1, @experiment_file_ops)
      experiment_record = FileProv.where("project_name": @project1["inode_name"], "i_name": @experiment_file_ops)
      expect(experiment_record.length).to eq 1
      prov_add_xattr(experiment_record[0], "appId", "#{@app_file_ops1}", "XATTR_ADD", 1)
      #pp experiment_record
      experiment_mock_file_1_op_1 = experiment_record[0].dup
      experiment_mock_file_1_op_1["inode_operation"] = "CREATE"
      experiment_mock_file_1_op_1["inode_id"] = 100000
      experiment_mock_file_1_op_1["io_logical_time"] = 0
      experiment_mock_file_1_op_1["io_timestamp"] = experiment_mock_file_1_op_1["io_timestamp"]+1
      experiment_mock_file_1_op_1["parent_i_id"] = experiment_record[0]["inode_id"]
      experiment_mock_file_1_op_1["i_name"] = "mock_file_1"
      experiment_mock_file_1_op_1["i_parent_name"] = experiment_record[0]["i_name"]
      experiment_mock_file_1_op_1["i_p1_name"] = experiment_record[0]["i_parent_name"]
      experiment_mock_file_1_op_1["io_app_id"] = @app_file_ops1
      experiment_mock_file_1_op_1.save!
      experiment_mock_file_1_op_2 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_1_op_2["inode_operation"] = "MODIFY_DATA"
      experiment_mock_file_1_op_2["io_logical_time"] = 1
      experiment_mock_file_1_op_2["io_timestamp"] = experiment_mock_file_1_op_2["io_timestamp"]+1
      experiment_mock_file_1_op_2.save!
      experiment_mock_file_2_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_2_op_1["inode_id"] = 100001
      experiment_mock_file_2_op_1["io_timestamp"] = experiment_mock_file_2_op_1["io_timestamp"]+1
      experiment_mock_file_2_op_1["i_name"] = "mock_file_2"
      experiment_mock_file_2_op_1.save!
      experiment_mock_file_3_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_3_op_1["inode_operation"] = "CREATE"
      experiment_mock_file_3_op_1["inode_id"] = 100002
      experiment_mock_file_3_op_1["io_timestamp"] = experiment_mock_file_3_op_1["io_timestamp"]+1
      experiment_mock_file_3_op_1["io_app_id"] = @app_file_ops2
      experiment_mock_file_3_op_1["i_name"] = "mock_file_3"
      experiment_mock_file_3_op_1.save!

      pp "restart epipe"
      execute_remotely @hostname, "sudo systemctl restart epipe"
      prov_wait_for_epipe()

      pp "check mock fileOperations"
      result = get_app_file_ops(@project1, @app_file_ops1, "NONE", "LIST")
      #pp result
      expect(result["items"].length).to eq 3
      expect(result["count"]).to eq 3
      result = get_app_file_ops(@project1, @app_file_ops1, "FILE_COMPACT", "LIST")
      # pp result
      expect(result.length).to eq 2

      pp "cleanup hops"
      prov_delete_experiment(@project1, @experiment_file_ops)

      pp "check hops cleanup"
      prov_wait_for_epipe()
      experiment_id1 = prov_experiment_id(@experiment_file_ops)
      check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id1, false)
    end

    it 'mock app footprint' do
      pp "stop epipe"
      execute_remotely @hostname, "sudo systemctl stop epipe"

      pp "create mock file operations - app footprint"
      prov_create_experiment(@project1, @experiment_app_fprint)
      experiment_record = FileProv.where("project_name": @project1["inode_name"], "i_name": @experiment_app_fprint)
      expect(experiment_record.length).to eq 1
      prov_add_xattr(experiment_record[0], "appId", "#{@app_app_fprint}", "XATTR_ADD", 1)
      #pp experiment_record

      experiment_mock_file_1_op_1 = experiment_record[0].dup
      experiment_mock_file_1_op_1["inode_operation"] = "CREATE"
      experiment_mock_file_1_op_1["inode_id"] = 200000
      experiment_mock_file_1_op_1["io_logical_time"] = 0
      experiment_mock_file_1_op_1["io_timestamp"] = experiment_mock_file_1_op_1["io_timestamp"]+1
      experiment_mock_file_1_op_1["parent_i_id"] = experiment_record[0]["inode_id"]
      experiment_mock_file_1_op_1["i_name"] = "mock_file_out_added_1"
      experiment_mock_file_1_op_1["i_parent_name"] = experiment_record[0]["i_name"]
      experiment_mock_file_1_op_1["i_p1_name"] = experiment_record[0]["i_parent_name"]
      experiment_mock_file_1_op_1["io_app_id"] = @app_app_fprint
      experiment_mock_file_1_op_1.save!

      experiment_mock_file_1_op_2 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_1_op_2["inode_operation"] = "MODIFY_DATA"
      experiment_mock_file_1_op_2["io_logical_time"] = experiment_mock_file_1_op_2["io_logical_time"]+1
      experiment_mock_file_1_op_2["io_timestamp"] = experiment_mock_file_1_op_2["io_timestamp"]+1
      experiment_mock_file_1_op_2.save!

      experiment_mock_file_2_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_2_op_1["inode_operation"] = "CREATE"
      experiment_mock_file_2_op_1["inode_id"] = 200001
      experiment_mock_file_2_op_1["io_logical_time"] = 0
      experiment_mock_file_2_op_1["io_timestamp"] = experiment_mock_file_2_op_1["io_timestamp"]+1
      experiment_mock_file_2_op_1["i_name"] = "mock_file_out_added_2"
      experiment_mock_file_2_op_1.save!

      experiment_mock_file_3_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_3_op_1["inode_operation"] = "MODIFY_DATA"
      experiment_mock_file_3_op_1["inode_id"] = 200002
      experiment_mock_file_3_op_1["io_logical_time"] = 1
      experiment_mock_file_3_op_1["io_timestamp"] = experiment_mock_file_3_op_1["io_timestamp"]+1
      experiment_mock_file_3_op_1["i_name"] = "mock_file_out_1"
      experiment_mock_file_3_op_1.save!

      experiment_mock_file_4_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_4_op_1["inode_operation"] = "CREATE"
      experiment_mock_file_4_op_1["inode_id"] = 200003
      experiment_mock_file_4_op_1["io_logical_time"] = 0
      experiment_mock_file_4_op_1["io_timestamp"] = experiment_mock_file_4_op_1["io_timestamp"]+1
      experiment_mock_file_4_op_1["i_name"] = "mock_file_tmp_1"
      experiment_mock_file_4_op_1.save!

      experiment_mock_file_4_op_2 = experiment_mock_file_4_op_1.dup
      experiment_mock_file_4_op_2["inode_operation"] = "DELETE"
      experiment_mock_file_4_op_2["io_logical_time"] = experiment_mock_file_4_op_2["io_logical_time"]+1
      experiment_mock_file_4_op_2["io_timestamp"] = experiment_mock_file_4_op_2["io_timestamp"]+1
      experiment_mock_file_4_op_2.save!

      experiment_mock_file_5_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_5_op_1["inode_operation"] = "DELETE"
      experiment_mock_file_5_op_1["inode_id"] = 200004
      experiment_mock_file_5_op_1["io_logical_time"] = 1
      experiment_mock_file_5_op_1["io_timestamp"] = experiment_mock_file_5_op_1["io_timestamp"]+1
      experiment_mock_file_5_op_1["i_name"] = "mock_file_rm_1"
      experiment_mock_file_5_op_1.save!

      experiment_mock_file_6_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_6_op_1["inode_operation"] = "ACCESS_DATA"
      experiment_mock_file_6_op_1["inode_id"] = 200005
      experiment_mock_file_6_op_1["io_logical_time"] = 1
      experiment_mock_file_6_op_1["io_timestamp"] = experiment_mock_file_6_op_1["io_timestamp"]+1
      experiment_mock_file_6_op_1["i_name"] = "mock_file_in_1"
      experiment_mock_file_6_op_1.save!

      pp "restart epipe"
      execute_remotely @hostname, "sudo systemctl restart epipe"
      prov_wait_for_epipe()

      pp "check mock app footprint"
      result = get_app_footprint(@project1, @app_app_fprint, "ALL")
      # pp result
      expect(result.length).to eq 6

      result = get_app_footprint(@project1, @app_app_fprint, "INPUT")
      # pp result
      expect(result.length).to eq 1

      result = get_app_footprint(@project1, @app_app_fprint, "OUTPUT")
      # pp result
      expect(result.length).to eq 3

      result = get_app_footprint(@project1, @app_app_fprint, "OUTPUT_ADDED")
      # pp result
      expect(result.length).to eq 2

      result = get_app_footprint(@project1, @app_app_fprint, "TMP")
      # pp result
      expect(result.length).to eq 1

      pp "cleanup hops"
      prov_delete_experiment(@project1, @experiment_app_fprint)

      pp "check hops cleanup"
      prov_wait_for_epipe()
      experiment_id1 = prov_experiment_id(@experiment_app_fprint)
      check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id1, false)
    end

    it 'mock file history' do
      pp "stop epipe"
      execute_remotely @hostname, "sudo systemctl stop epipe"

      pp "create mock file history"
      prov_create_experiment(@project1, @experiment_file_history)
      experiment_record = FileProv.where("project_name": @project1["inode_name"], "i_name": @experiment_file_history)
      expect(experiment_record.length).to eq 1
      prov_add_xattr(experiment_record[0], "appId", "#{@app_file_history1}", "XATTR_ADD", 1)
      #pp experiment_record

      experiment_mock_file_1_op_1 = experiment_record[0].dup
      experiment_mock_file_1_op_1["inode_operation"] = "CREATE"
      experiment_mock_file_1_op_1["inode_id"] = 300000
      experiment_mock_file_1_op_1["io_logical_time"] = 0
      experiment_mock_file_1_op_1["io_timestamp"] = experiment_mock_file_1_op_1["io_timestamp"]+1
      experiment_mock_file_1_op_1["parent_i_id"] = experiment_record[0]["inode_id"]
      experiment_mock_file_1_op_1["i_name"] = "mock_file_1"
      experiment_mock_file_1_op_1["i_parent_name"] = experiment_record[0]["i_name"]
      experiment_mock_file_1_op_1["i_p1_name"] = experiment_record[0]["i_parent_name"]
      experiment_mock_file_1_op_1["io_app_id"] = @app_file_history1
      experiment_mock_file_1_op_1.save!

      experiment_mock_file_1_op_2 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_1_op_2["inode_operation"] = "MODIFY_DATA"
      experiment_mock_file_1_op_2["io_logical_time"] = experiment_mock_file_1_op_2["io_logical_time"]+1
      experiment_mock_file_1_op_2["io_timestamp"] = experiment_mock_file_1_op_2["io_timestamp"]+1
      experiment_mock_file_1_op_2.save!

      experiment_mock_file_1_op_3 = experiment_mock_file_1_op_2.dup
      experiment_mock_file_1_op_3["inode_operation"] = "ACCESS_DATA"
      experiment_mock_file_1_op_3["io_app_id"] = @app_file_history2
      experiment_mock_file_1_op_3["io_logical_time"] = experiment_mock_file_1_op_3["io_logical_time"]+1
      experiment_mock_file_1_op_3["io_timestamp"] = experiment_mock_file_1_op_3["io_timestamp"]+1
      experiment_mock_file_1_op_3.save!

      experiment_mock_file_1_op_4 = experiment_mock_file_1_op_3.dup
      experiment_mock_file_1_op_4["inode_operation"] = "ACCESS_DATA"
      experiment_mock_file_1_op_4["io_app_id"] = @app_file_history3
      experiment_mock_file_1_op_4["io_logical_time"] = experiment_mock_file_1_op_4["io_logical_time"]+1
      experiment_mock_file_1_op_4["io_timestamp"] = experiment_mock_file_1_op_4["io_timestamp"]+1
      experiment_mock_file_1_op_4.save!

      experiment_mock_file_1_op_5 = experiment_mock_file_1_op_4.dup
      experiment_mock_file_1_op_5["inode_operation"] = "DELETE"
      experiment_mock_file_1_op_5["io_logical_time"] = experiment_mock_file_1_op_5["io_logical_time"]+1
      experiment_mock_file_1_op_5["io_timestamp"] = experiment_mock_file_1_op_5["io_timestamp"]+1
      experiment_mock_file_1_op_5.save!

      pp "restart epipe"
      execute_remotely @hostname, "sudo systemctl restart epipe"
      prov_wait_for_epipe() 

      pp "check file history" 
      result = get_file_ops(@project1, 300000, "NONE", "LIST")
      #pp result
      expect(result["items"].length).to eq 5
      expect(result["count"]).to eq 5

      pp "cleanup hops"
      prov_delete_experiment(@project1, @experiment_file_history)

      pp "check hops cleanup" 
      prov_wait_for_epipe() 
      experiment_id1 = prov_experiment_id(@experiment_file_history)
      check_no_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id1, false)
    end
  end

  # describe "test" do
  #   before :all do
  #     pp "create project: #{@project1_name}"
  #     @project1 = create_project_by_name(@project1_name)
  #   end

  #   after :each do
  #     pp "cleanup provenance index"
  #     sleep(1)
  #     ops = get_file_ops_archival(@project1)
  #     #pp ops
  #     cleanup(@project1, ops)

  #     pp "check provenance index cleanup"
  #     sleep(1)
  #     ops = get_file_ops_archival(@project1)
  #     #pp ops
  #     expect(ops["count"]).to eq 0
  #   end

  #   after :all do 
  #     pp "delete projects"
  #     delete_project(@project1)
  #     @project1 = nil
  #   end

  #   it "experiment artifact footprint" do
  #     pp "check epipe"
  #     execute_remotely @hostname, "sudo systemctl restart epipe"

  #     prov_create_experiment(@project1, "#{@app1_id}_1")
  #     prov_create_experiment(@project1, "#{@app1_id}_2")

  #     pp "cleanup hops"
  #     prov_delete_experiment(@project1, "#{@app1_id}_1")
  #     prov_delete_experiment(@project1, "#{@app1_id}_2")

  #     pp "artifact footprint"

  #     pp "check cleanup" 
  #     prov_wait_for_epipe() 
  #     check_no_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app1_name1), false)
  #     check_no_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app2_name1), false)
  #   end
  # end

  # describe "test2" do
  #   before :all do
  #     pp "create project: #{@project1_name}"
  #     @project1 = create_project_by_name(@project1_name)
  #   end

    # after :each do
    #   pp "cleanup provenance index"
    #   sleep(1)
    #   ops = get_file_ops_archival(@project1)
    #   #pp ops
    #   cleanup(@project1, ops)

    #   pp "check provenance index cleanup"
    #   sleep(1)
    #   ops = get_file_ops_archival(@project1)
    #   #pp ops
    #   expect(ops["count"]).to eq 0
    # end

    # after :all do 
    #   pp "delete projects"
    #   delete_project(@project1)
    #   @project1 = nil
    # end

    # it "epipe test" do
    #   pp "check epipe"
    #   execute_remotely @hostname, "sudo systemctl restart epipe"

    #   experiment1_id = prov_experiment_id("#{@app1_id}_1");
    #   prov_create_experiment(@project1, experiment1_id)

    #   experiment_record = FileProv.where("project_name": @project1["inode_name"], "i_name": experiment1_id)
    #   expect(experiment_record.length).to eq 1
    #   prov_add_xattr(experiment_record[0], "xattr1", 99, "XATTR_ADD", 1)
      #pp experiment_record

      # pp "cleanup hops"
      # prov_delete_experiment(@project1, "#{@app1_id}_1")

      # pp "check cleanup" 
      # prov_wait_for_epipe() 
      # check_no_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app1_name1), false)
  #   end
  # end

  # describe 'test suite - break project' do
  #   before :all do
  #     pp "create project: #{@project1_name}"
  #     @project1 = create_project_by_name(@project1_name)
  #   end

  #   after :all do 
  #     pp "delete projects"
  #     delete_project(@project1)
  #     @project1 = nil
  #   end
  #   it 'delete Experiments dataset - check cleanup' do
  #     pp "restart epipe"
  #     execute_remotely @hostname, "sudo systemctl restart epipe"

  #     pp "create experiments"
  #     prov_create_experiment(@project1, @experiment_app1_name1)
  #     prov_create_experiment(@project1, @experiment_app2_name1)

  #     pp "wait for epipe"
  #     prov_wait_for_epipe() 

  #     pp "check experiments" 
  #     get_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app1_name1), false)
  #     get_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app2_name1), false)

  #     pp "delete Experiments dataset"
  #     prov_delete_dataset(@project1, "Experiments")

  #     pp "check experiments" 
  #     prov_wait_for_epipe() 
  #     check_no_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app1_name1), false)
  #     check_no_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app2_name1), false)

  #     pp "cleanup provenance index"
  #     sleep(1)
  #     ops = get_file_ops_archival(@project1)
  #     #pp ops
  #     cleanup(@project1, ops)

  #     pp "check provenance index cleanup"
  #     sleep(1)
  #     ops = get_file_ops_archival(@project1)
  #     #pp ops
  #     expect(ops["count"]).to eq 0
  #   end
  # end

  # describe 'test suite - break project' do
  #   before :all do
  #     pp "create project: #{@project1_name}"
  #     @project1 = create_project_by_name(@project1_name)
  #   end

  #   it 'delete Project - check cleanup' do
  #     pp "restart epipe"
  #     execute_remotely @hostname, "sudo systemctl restart epipe"

  #     pp "create experiments"
  #     prov_create_experiment(@project1, @experiment_app1_name1)
  #     prov_create_experiment(@project1, @experiment_app2_name1)

  #     pp "wait for epipe"
  #     prov_wait_for_epipe() 

  #     pp "check experiments" 
  #     get_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app1_name1), false)
  #     get_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app2_name1), false)

  #     pp "delete Project"
  #     delete_project(@project1)

  #     pp "check experiments" 
  #     prov_wait_for_epipe() 
  #     #check_no_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app1_name1), false)
  #     #check_no_ml_asset_by_id(@project1, "EXPERIMENT", prov_experiment_id(@experiment_app2_name1), false)

  #     pp "cleanup provenance index"
  #     sleep(1)
  #     ops = get_file_ops_archival(@project1)
  #     #pp ops
  #     cleanup(@project1, ops)

  #     pp "check provenance index cleanup"
  #     sleep(1)
  #     ops = get_file_ops_archival(@project1)
  #     #pp ops
  #     expect(ops["count"]).to eq 0
  #     @project1 = nil
  #   end
  # end
end