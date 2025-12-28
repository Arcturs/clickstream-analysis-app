package ru.spb.itmo.pirsbd.asashina.api.repository

import org.springframework.data.cassandra.repository.CassandraRepository
import org.springframework.stereotype.Repository
import ru.spb.itmo.pirsbd.asashina.api.model.entity.ClickEventBySession
import ru.spb.itmo.pirsbd.asashina.api.model.entity.ClickEventBySessionKey

@Repository
interface ClickEventBySessionRepository : CassandraRepository<ClickEventBySession, ClickEventBySessionKey>