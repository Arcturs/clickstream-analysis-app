package ru.spb.itmo.pirsbd.asashina.api.repository

import org.springframework.data.cassandra.repository.CassandraRepository
import org.springframework.stereotype.Repository
import ru.spb.itmo.pirsbd.asashina.api.model.entity.ClickEvent
import ru.spb.itmo.pirsbd.asashina.api.model.entity.ClickEventKey

@Repository
interface ClickEventRepository : CassandraRepository<ClickEvent, ClickEventKey>