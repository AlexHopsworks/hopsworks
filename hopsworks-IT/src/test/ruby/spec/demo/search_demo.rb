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

describe "On #{ENV['OS']}" do
  before(:all) do
    @demo = false
  end

  def create_and_wait_for_featurestore_tour()
    @project = nil
    with_valid_tour_project("featurestore")
    get_executions(project[:id], get_featurestore_tour_job_name, "")
    expect_status_details(200)
    expect(json_body["items"].count == 1).to be true
    execution = parsed_json["items"][0]

    wait_for_execution(2000) do
      get_execution(project[:id], get_featurestore_tour_job_name, execution["id"])
      expect_status_details(200)
      pp "project:#{project[:projectname]} featurestore tour job:#{json_body["state"]}"
      not is_execution_active(json_body)
    end
  end

  it 'simple 2 users, 3 projects with shared featurestores and training datasets' do
    user1_params = {}
    user1_params[:email] = "demo1@logicalclocks.com"
    user1_params[:password] = "demo123"
    create_user(user1_params)
    user2_params = {}
    user2_params[:email] = "demo2@logicalclocks.com"
    user2_params[:password] = "demo123"
    create_user(user2_params)

    create_session(user1_params[:email], user1_params[:password])
    create_and_wait_for_featurestore_tour()
    project1 = @project

    create_session(user2_params[:email], user2_params[:password])
    create_and_wait_for_featurestore_tour()
    project2 = @project
    featurestore2_name = project2[:projectname].downcase + "_featurestore.db"
    featurestore2 = get_dataset(project2, featurestore2_name)

    #share featurestore2 of user2 with user1
    create_session(user1_params[:email], user1_params[:password])
    request_access_by_dataset(featurestore2, project1)
    create_session(user2_params[:email], user2_params[:password])
    share_dataset_checked(project2, featurestore2_name, project1[:projectname], "FEATURESTORE")
  end
end