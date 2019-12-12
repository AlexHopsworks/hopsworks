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
module ProvOpsHelper
  def get_file_ops(project, inode_id, compaction, return_type)
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/ops/#{inode_id}"
    query_params = "?compaction=#{compaction}&return_type=#{return_type}"
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
  end

  def get_file_ops_archival(project)
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/ops"
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

  def get_app_file_ops(project, app_id, compaction, return_type)
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/ops"
    query_params = "?filter_by=APP_ID:#{app_id}&compaction=#{compaction}&return_type=#{return_type}"
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
  end

  def get_file_oldest_deleted(limit)
    resource = "#{ENV['HOPSWORKS_API']}/provenance/ops"
    query_params = "?filter_by=FILE_OPERATION:DELETE&sort_by=TIMESTAMP:asc&limit=#{limit}"
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
  end

  def get_app_footprint(project, app_id, type)
    resource = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/provenance/footprint/#{type}/app/#{app_id}"
    query_params = ""
    pp "#{resource}#{query_params}"
    result = get "#{resource}#{query_params}"
    expect_status(200)
    parsed_result = JSON.parse(result)
    parsed_result["items"]
  end
end