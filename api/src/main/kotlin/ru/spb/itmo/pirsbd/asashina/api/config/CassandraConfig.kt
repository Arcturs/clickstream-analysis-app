package ru.spb.itmo.pirsbd.asashina.api.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Configuration
import org.springframework.data.cassandra.config.AbstractCassandraConfiguration
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories


@Configuration
@EnableCassandraRepositories(
    basePackages = ["ru.spb.itmo.pirsbd.asashina.api.repository"],
    cassandraTemplateRef = "cassandraTemplate"
)
class CassandraConfig(
    @Value("\${spring.cassandra.contact-points}") private val contactPoints: String,
    @Value("\${spring.cassandra.port}") private val port: Int,
    @Value("\${spring.cassandra.keyspace-name}") private val keyspaceName: String,
    @Value("\${spring.cassandra.local-datacenter}") private val localDatacenter: String,
) : AbstractCassandraConfiguration() {

    override fun getContactPoints() = contactPoints

    override fun getPort() = port

    override fun getKeyspaceName() = keyspaceName

    override fun getLocalDataCenter() = localDatacenter

}