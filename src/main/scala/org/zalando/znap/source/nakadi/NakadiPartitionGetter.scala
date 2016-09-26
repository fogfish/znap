/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.source.nakadi

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.ActorMaterializer
import org.zalando.znap.config.NakadiSource
import org.zalando.znap.objects.Partition
import org.zalando.znap.source.nakadi.objects.NakadiPartition

import scala.concurrent.Future

class NakadiPartitionGetter(nakadiSource: NakadiSource,
                            tokens: NakadiTokens)
                           (implicit actorSystem: ActorSystem){
  import org.zalando.znap.utils.RichStream._

  private val http = Http(actorSystem)
  private implicit val executionContext = actorSystem.dispatcher
  private implicit val materializer = ActorMaterializer()

  def getPartitions: Future[List[Partition]] = {
    // Request partitions of the topic.
    val scheme = nakadiSource.uri.getScheme
    val hostAndPort = nakadiSource.uri.getAuthority
    val uri = s"$scheme://$hostAndPort/event-types/${nakadiSource.eventType}/partitions"
    val authorizationHeader = new Authorization(OAuth2BearerToken(tokens.get()))
    val request = HttpRequest(
      uri = uri,
      headers = List(authorizationHeader))

    http.singleRequest(request).flatMap {
      case HttpResponse(StatusCodes.OK, _, entity, _) =>
        entity.dataBytes.collectAsObject[List[NakadiPartition]].map { nakadiPartitions =>
          val partitions = nakadiPartitions.map { np =>
            Partition(np.partition, np.oldestAvailableOffset, np.newestAvailableOffset)
          }
          partitions
        }

      case unknownResponse: HttpResponse =>
        unknownResponse.entity.dataBytes.collectAsString().map { content =>
          throw new Exception(s"Error response on getting partitions, ${unknownResponse.status} $content")
        }
    }
  }
}
