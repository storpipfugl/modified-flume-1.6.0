package org.flume.source.sql;

import java.util.List;
import java.util.Map;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.transform.Transformers;

public class HibernateQuery {
	private Session session;

	HibernateQuery(String dialect, String driverClassName, String url, String username, String password) {
		Configuration configuration = new Configuration().setProperty("hibernate.dialect", dialect).setProperty("hibernate.connection.driver_class", driverClassName).setProperty("hibernate.connection.url", url).setProperty("hibernate.connection.username", username).setProperty("hibernate.connection.password", password);

		ServiceRegistry serviceRegistry = new StandardServiceRegistryBuilder().applySettings(configuration.getProperties()).build();
		SessionFactory sessionFactory = configuration.buildSessionFactory(serviceRegistry);
		session = sessionFactory.openSession();
	}

	@SuppressWarnings("unchecked")
	public List<Map<String,Object>> select(String table, String columns,String indexColumn, int readedIndex) {
		return session.createSQLQuery("select " + columns+","+indexColumn+" indexColumn from " + table +" where "+indexColumn+" > "+ readedIndex).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
	}
	
	public int selectMaxIndex(String table,String indexColumn) {
		return Integer.valueOf(session.createSQLQuery("select MAX(" + indexColumn + ") from " + table).uniqueResult().toString());
	}
}
