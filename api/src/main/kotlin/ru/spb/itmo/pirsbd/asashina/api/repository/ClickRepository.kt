package ru.spb.itmo.pirsbd.asashina.api.repository

import org.springframework.data.cassandra.repository.CassandraRepository
import org.springframework.stereotype.Repository
import ru.spb.pirsbd.asashina.common.dto.ClickEvent

@Repository
interface ClickRepository : CassandraRepository<ClickEvent, String>