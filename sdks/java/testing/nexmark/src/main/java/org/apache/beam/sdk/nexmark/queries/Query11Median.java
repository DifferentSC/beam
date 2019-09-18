/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.nexmark.queries;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.BidsPerSession;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.util.Utility;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.util.Comparator;
import java.util.List;

/**
 * Query "11", 'User sessions with median operation' (Not in original suite.)
 *
 * <p>Group bids by the same user into sessions with {@code windowSizeSec} max gap. However limit
 * the session to at most {@code maxLogEvents}. Emit the number of bids per session.
 */
public class Query11Median extends NexmarkQueryTransform<BidsPerSession> {
	private final NexmarkConfiguration configuration;

	public Query11Median(NexmarkConfiguration configuration) {
		super("Query11Median");
		this.configuration = configuration;
	}

	@Override
	public PCollection<BidsPerSession> expand(PCollection<Event> events) {
		PCollection<KV<Long, Bid>> bidders =
				events
						.apply(NexmarkQueryUtil.JUST_BIDS)
						.apply(
								WithKeys.of(new SerializableFunction<Bid, Long>() {
									@Override
									public Long apply(Bid input) {
										return input.bidder;
									}
								}));

		PCollection<KV<Long, Bid>> biddersWindowed =
				bidders.apply(
						Window.into(
								Sessions.withGapDuration(Duration.standardSeconds(configuration.windowSizeSec))));

		return biddersWindowed
				.apply(GroupByKey.create())
				.apply(
						name + ".ToResult",
						ParDo.of(
								new DoFn<KV<Long, Iterable<Bid>>, BidsPerSession>() {
									@ProcessElement
									public void processElement(ProcessContext c) {
										final List<Bid> list = (List<Bid>) (c.element().getValue());
										final Bid medianBid = Utility.kthSmallestElement(list, list.size() / 2,
												Comparator.comparingLong(x -> x.price));
										c.output(new BidsPerSession(c.element().getKey(), medianBid.price));
									}
								}
						)
				);
	}
}
