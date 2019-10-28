=begin
 This file is part of Hopsworks
 Copyright (C) 2019, Logical Clocks AB. All rights reserved

 Hopsworks is free software: you can redistribute it and/or modify it under the terms of
 the GNU Affero General Public License as published by the Free Software Foundation,
 either version 3 of the License, or (at your option) any later version.

 Hopsworks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 PURPOSE.  See the GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/>.
=end
module ProvenanceHelper
  def check_archive_disabled
    target = "#{ENV['HOPSWORKS_API']}/provenance/settings/archive"
    pp "get #{target}"
    result = get target
    expect_status(200)
    parsed_result = JSON.parse(result)
    expect(parsed_result["result"]["value"]).to eq 0
  end

  def cleanup_cycle(project)
    prov_wait_for_epipe()
    sleep(1)
    ops = get_file_ops_archival(project)
    #pp ops
    ops["filesInProject"]["items"].each do |file|
      aux = file_ops_archival(project, file["inodeId"])
      # pp "#{aux} #{file["count"]}"
      expect(aux["cleaned"]).to eq file["count"]
    end

    sleep(1)
    get_file_ops_archival(project)
  end

  def prov_create_dir(project, dirname) 
    target = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/dataset"
    payload_string = '{"name": "' + dirname + '"}'
    payload = JSON.parse(payload_string)
    pp "post #{target}, #{payload}"
    post target, payload
    expect_status(200)
  end

  def prov_create_experiment(project, experiment_name) 
      pp "create experiment #{experiment_name} in project #{project[:inode_name]}"
      prov_create_dir(project, "Experiments/#{experiment_name}")
  end

  def prov_create_model(project, model_name) 
    pp "create model #{model_name} in project #{project[:inode_name]}"
    models = "Models"
    prov_create_dir(project, "#{models}/#{model_name}")
  end

  def prov_create_model_version(project, model_name, model_version) 
    pp "create model #{model_name}_#{model_version} in project #{project[:inode_name]}"
    models = "Models"
    prov_create_dir(project, "#{models}/#{model_name}/#{model_version}")
  end

  def prov_create_td(project, td_name, td_version) 
    pp "create training dataset #{td_name}_#{td_version} in project #{project[:inode_name]}"
    training_datasets = "#{project[:inode_name]}_Training_Datasets"
    prov_create_dir(project, "#{training_datasets}/#{td_name}_#{td_version}")
  end
  
  def prov_delete_dir(project, dirname) 
    target = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/dataset/file/#{dirname}"
    delete target
    expect_status(200)
  end

  def prov_delete_dataset(project, dataset)
    target = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/dataset/#{dataset}"
    delete target
    expect_status(200)
  end 

  def prov_delete_experiment(project, experiment_name) 
    pp "delete experiment #{experiment_name} in project #{project[:inode_name]}"
    experiments = "Experiments"
    prov_delete_dir(project, "#{experiments}/#{experiment_name}")
  end

  def prov_delete_model(project, model_name) 
    pp "delete model #{model_name} in project #{project[:inode_name]}"
    models = "Models"
    prov_delete_dir(project, "#{models}/#{model_name}")
  end

  def prov_delete_td(project, td_name, td_version) 
    pp "delete training dataset #{td_name}_#{td_version} in project #{project[:inode_name]}"
    training_datasets = "#{project[:inode_name]}_Training_Datasets"
    prov_delete_dir(project, "#{training_datasets}/#{td_name}_#{td_version}")
  end

  def prov_experiment_id(experiment_name)
    "#{experiment_name}"
  end

  def prov_model_id(model_name, model_version)
    "#{model_name}_#{model_version}"
  end

  def prov_td_id(td_name, td_version)
    "#{td_name}_#{td_version}"
  end

  def prov_get_td_record(project, td_name, td_version) 
    training_datasets = "#{project[:inode_name]}_Training_Datasets"
    training_dataset = prov_td_id(td_name, td_version)
    FileProv.where("project_name": project["inode_name"], "i_parent_name": training_datasets, "i_name": training_dataset)
  end

  def prov_get_experiment_record(project, experiment_name) 
    experiment_parent = "Experiments"
    FileProv.where("project_name": project["inode_name"], "i_parent_name": experiment_parent, "i_name": experiment_name)
  end

  def prov_add_xattr(original, xattr_name, xattr_value, xattr_op, increment)
#     pp original
    xattrRecord = original.dup
#     pp xattrRecord
    xattrRecord[:inode_id] = original[:inode_id]
    xattrRecord[:inode_operation] = xattr_op
    xattrRecord[:io_logical_time] = original[:io_logical_time]+increment
    xattrRecord[:io_timestamp] = original[:io_timestamp]+increment
    xattrRecord[:io_app_id] = original[:io_app_id]
    xattrRecord[:io_user_id] = original[:io_user_id]
#     pp xattrRecord
    xattrRecord["i_xattr_name"] = xattr_name
    xattrRecord["io_logical_time_batch"] = original["io_logical_time_batch"]+increment
    xattrRecord["io_timestamp_batch"] = original["io_timestamp_batch"]+increment
#     pp xattrRecord
    xattrRecord.save!

    FileProvXAttr.create(inode_id: xattrRecord["inode_id"], namespace: 5, name: xattr_name, inode_logical_time: xattrRecord["io_logical_time"], value: xattr_value)
  end

  def prov_add_app_states1(app_id, user)
    timestamp = Time.now
    AppProv.create(id: app_id, state: "null", timestamp: timestamp, name: app_id, user: user, submit_time: timestamp-10, start_time: timestamp-5, finish_time: 0)
    AppProv.create(id: app_id, state: "null", timestamp: timestamp+5, name: app_id, user: user, submit_time: timestamp-10, start_time: timestamp-5, finish_time: 0)
  end
  def prov_add_app_states2(app_id, user)
    timestamp = Time.now
    AppProv.create(id: app_id, state: "null", timestamp: timestamp, name: app_id, user: user, submit_time: timestamp-10, start_time: timestamp-5, finish_time: 0)
    AppProv.create(id: app_id, state: "null", timestamp: timestamp+5, name: app_id, user: user, submit_time: timestamp-10, start_time: timestamp-5, finish_time: 0)
    AppProv.create(id: app_id, state: "FINISHED", timestamp: timestamp+10, name: app_id, user: user, submit_time: timestamp-10, start_time: timestamp-5, finish_time: timestamp+50)
  end

  def prov_wait_for_epipe() 
    pp "waiting"
    sleepCounter1 = 0
    sleepCounter2 = 0
    until FileProv.all.empty? || sleepCounter1 == 10 do
      sleep(1)
      sleepCounter1 += 1
    end
    until AppProv.all.empty? || sleepCounter2 == 10 do
      sleep(1)
      sleepCounter2 += 1
    end
    sleep(2)
    expect(sleepCounter1).to be < 10
    expect(sleepCounter2).to be < 10
    pp "done waiting"
  end

  def prov_check_experiment3(experiments, experiment_id, currentState) 
    experiment = experiments.select { |e| e["mlId"] == experiment_id }
    expect(experiment.length).to eq 1

    #pp experiment[0]["appState"]
    expect(experiment[0]["appState"]["currentState"]).to eq currentState
  end

  def prov_check_asset_with_id(assets, asset_id) 
    asset = assets.select {|a| a["mlId"] == asset_id }
    expect(asset.length).to eq 1
    #pp asset
  end 

  def prov_check_asset_with_xattrs(assets, asset_id, xattrs)
    asset = assets.select {|a| a["mlId"] == asset_id }
    expect(asset.length).to eq 1
    #pp model
    expect(asset[0]["xattrs"]["entry"].length).to eq xattrs.length
    xattrs.each do |key, value|
      #pp model[0]["xattrs"]["entry"]
      xattr = asset[0]["xattrs"]["entry"].select do |e|
        e["key"] == key && e["value"] == value
      end
      expect(xattr.length).to eq 1
      #pp xattr
    end
  end 

  def get_ml_asset_in_project(project, mlType, withAppState, expected)
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/state"
    query_params = "?filter_by=ML_TYPE:#{mlType}"
    if withAppState
      query_params = query_params + "&expand=APP"
    end
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    #pp result
    expect_status(200)
    parsed_result = JSON.parse(result)
    expect(parsed_result["items"].length).to eq expected
    expect(parsed_result["count"]).to eq expected
    parsed_result
  end

  @file_state = ->(project){"#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/state"}

  def ProvenanceHelper.ml_type(type)
    ->(url) { "#{url}filter_by=ML_TYPE:#{type}" }
  end

  @prov_in_proj = ->(project) {
    "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}"
  }

  @qwith = ->(url) { "#{url}?" }
  @qand = ->(url) { "#{url}&" }
  @experiment = ml_type("EXPERIMENT")
  @app_expand = ->(url) { "#{url}expand=APP" }

  @prov_file_ops = ->(url, inode_id) {
    "#{url}/provenance/file/#{inode_id}/ops"
  }

  @prov_file_ops_g = ->(url) {
    "#{url}/provenance/file/ops"
  }

  @prov_cleanup = ->(url) {
    "#{url}/cleanup"
  }

  def get_file_ops_archival(project)
    resource_f = @prov_in_proj << @prof_file_ops_g << @prov_cleanup
    resource = resource_f.call(project)
    pp resource
    # resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/ops"
    query_params = "?return_type=COUNT&aggregations=FILES_IN"
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
  end

  def get_ml_asset_in_project_page(project, mlType, withAppState, offset, limit)
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/state"
    query_params = "?filter_by=ML_TYPE:#{mlType}&offset=#{offset}&limit=#{limit}"
    if withAppState
      query_params = query_params + "&expand=APP"
    end
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
    parsed_result
  end

  def check_no_ml_asset_by_id(project, mlType, mlId, withAppState)
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/state"
    query_params = "?filter_by=ML_TYPE:#{mlType}&filter_by=ML_ID:#{mlId}"
    if withAppState
      query_params = query_params + "&expand=APP"
    end
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
    expect(parsed_result["items"].length).to eq 0
    expect(parsed_result["count"]).to eq 0
  end

  def get_ml_asset_by_id(project, mlType, mlId, withAppState)
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/state"
    query_params = "?filter_by=ML_TYPE:#{mlType}&filter_by=ML_ID:#{mlId}"
    if withAppState
      query_params = query_params + "&expand=APP"
    end
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
    expect(parsed_result["items"].length).to eq 1
    expect(parsed_result["count"]).to eq 1
    parsed_result["items"][0]
  end

  def get_ml_asset_like_name(project, mlType, term)
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/state"
    query_params = "?filter_by=ML_TYPE:#{mlType}&filter_by=FILE_NAME_LIKE:#{term}"
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
    parsed_result["items"]
  end

  def get_ml_in_create_range(project, mlType, from, to, expected)
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/state"
    query_params = "?filter_by=ML_TYPE:#{mlType}&filter_by=CREATE_TIMESTAMP_LT:#{to}&filter_by=CREATE_TIMESTAMP_GT:#{from}"
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
    #pp parsed_result
    expect(parsed_result["items"].length).to eq expected
    expect(parsed_result["count"]).to eq expected
    parsed_result["items"]
  end

  def get_ml_asset_by_xattr(project, mlType, xattr_key, xattr_val)
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/state"
    query_params = "?filter_by=ML_TYPE:#{mlType}&xattr_filter_by=#{xattr_key}:#{xattr_val}"
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
    parsed_result["items"]
  end

  def get_ml_asset_by_xattr_count(project, mlType, xattr_key, xattr_val, count)
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/state"
    query_params = "?filter_by=ML_TYPE:#{mlType}&xattr_filter_by=#{xattr_key}:#{xattr_val}&return_type=COUNT"
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
    expect(parsed_result["result"]["value"]).to eq count
    parsed_result["result"]
  end

  def get_ml_asset_like_xattr(project, mlType, xattr_key, xattr_val)
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/state"
    query_params = "?filter_by=ML_TYPE:#{mlType}&xattr_like=#{xattr_key}:#{xattr_val}"
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
    parsed_result["items"]
  end
    
  def get_ml_td_count_using_feature_project(project, feature_name) 
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/state"
    query_params = "?filter_by=ML_TYPE:TRAINING_DATASET&xattr_filter_by=features.name:#{feature_name}&return_type=COUNT"
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
    parsed_result["items"]
  end

  def get_ml_td_count_using_feature_global(feature_name) 
    resource = "#{ENV['HOPSWORKS_API']}/provenance/file/state"
    query_params = "?filter_by=ML_TYPE:TRAINING_DATASET&xattr_filter_by==features.name:#{feature_name}&return_type=COUNT"
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
    parsed_result["items"]
  end

  def get_file_ops(project, inodeId, compaction, return_type) 
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/#{inodeId}/ops"
    query_params = "?compaction=#{compaction}&return_type=#{return_type}"
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
  end

  def get_file_ops_archival(project)
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/ops"
    query_params = "?return_type=COUNT&aggregations=FILES_IN"
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
  end

  def file_ops_archival(project, inode_id)
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/#{inode_id}/ops/cleanup"
    pp "#{resource}"
    result = delete "#{resource}"
    expect_status(200)
    parsed_result = JSON.parse(result)
  end

  def get_app_file_ops(project, appId, compaction, return_type) 
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/file/ops"
    query_params = "?filter_by=APP_ID:#{appId}&compaction=#{compaction}&return_type=#{return_type}"
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
  end

  def get_app_footprint(project, appId, type) 
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/app/#{appId}/footprint/#{type}"
    query_params = ""
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
    parsed_result["items"]
  end

  def get_file_oldest_deleted(limit) 
    resource = "#{ENV['HOPSWORKS_API']}/provenance/file/ops"
    query_params = "?filter_by=FILE_OPERATION:DELETE&sort_by=TIMESTAMP:asc&limit=#{limit}"
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
    parsed_result
  end

  def attach_app_id_xattr(project, expName, appId)
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/test/exp/#{expName}/#{appId}"
    pp "#{resource}"
    result = post "#{resource}"
    expect_status(200)
  end
end