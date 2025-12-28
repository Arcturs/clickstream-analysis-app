package ru.spb.itmo.pirsbd.asashina.api.repository

import org.springframework.data.cassandra.repository.CassandraRepository
import org.springframework.stereotype.Repository
import ru.spb.itmo.pirsbd.asashina.api.model.entity.ClickEventByType
import ru.spb.itmo.pirsbd.asashina.api.model.entity.ClickEventByTypeKey

@Repository
interface ClickEventByTypeRepository : CassandraRepository<ClickEventByType, ClickEventByTypeKey>