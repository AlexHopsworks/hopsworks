/*
 * This file is part of Hopsworks
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
 *
 * Hopsworks is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * Hopsworks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */
package io.hops.hopsworks.common.provenance.elastic.core;

import io.hops.hopsworks.common.util.functional.CheckedFunction;

/**
 * @param <R> Parses the elastic response BasicElasticHit into type R.
 * @param <E> Exception that can be thrown during the parsing.
 */
public interface ElasticHitParser<R, E extends Exception> extends CheckedFunction<BasicElasticHit, R, E> {
}
