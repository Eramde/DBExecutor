/*
 * Copyright (c) 2016, sot
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package tk.sottech.dbexecutor;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import static tk.sottech.misc.Misc.ownStack;

/**
 *
 * @author sot
 */
public final class DBUtility {

	private final Map<Class, DBTypeConverter> converters = DEFAULT_CONVERTERS;
	public static final Map<Class, DBTypeConverter> DEFAULT_CONVERTERS;
	private String url, login, password;
	private Map<Class, SQLType> customSqlTypeMap = DEFAULT_SQL_TYPE_MAP;
	public static final Map<Class, SQLType> DEFAULT_SQL_TYPE_MAP;
	private boolean fixAutoCommitIssue = false;
	private static final Logger LOG = Logger.getLogger(DBUtility.class.getName());

	static {
		HashMap<Class, JDBCType> tmp = new HashMap<>();
		tmp.put(String.class, JDBCType.VARCHAR);
		tmp.put(BigDecimal.class, JDBCType.NUMERIC);
		tmp.put(Boolean.class, JDBCType.BIT);
		tmp.put(Byte.class, JDBCType.TINYINT);
		tmp.put(Short.class, JDBCType.SMALLINT);
		tmp.put(Integer.class, JDBCType.INTEGER);
		tmp.put(Long.class, JDBCType.BIGINT);
		tmp.put(Float.class, JDBCType.REAL);
		tmp.put(Double.class, JDBCType.DOUBLE);
		tmp.put(byte[].class, JDBCType.VARBINARY);
		tmp.put(Date.class, JDBCType.DATE);
		tmp.put(Time.class, JDBCType.TIME);
		tmp.put(Timestamp.class, JDBCType.TIMESTAMP);

		DEFAULT_SQL_TYPE_MAP = Collections.unmodifiableMap(tmp);

		HashMap<Class, DBTypeConverter> tmp0 = new HashMap<>();
		tmp0.put(ResultSet.class, new DBTypeConverter<ResultSet>() {

				 private final DBTypeConverter<ResultSet> RESULT_SET_CONVERTER = (ResultSet data,
																				  String columnName) -> {
					 try {
						 return convertResultSetToHashMaps(data, null);
					 } catch (SQLException ex) {
						 LOG.log(Level.SEVERE, ownStack(ex));
						 return new ArrayList<>();
					 }
				 };

				 @Override
				 public Object convert(ResultSet data, String columnName) {
					 return RESULT_SET_CONVERTER.convert(data, columnName);
				 }
			 });
		DEFAULT_CONVERTERS = Collections.unmodifiableMap(tmp0);
	}

	private DBUtility() {
	}

	public static DBUtility create(String urlOrDriver) throws SQLException,
															  ClassNotFoundException {
		DBUtility dbu = new DBUtility();
		if (urlOrDriver != null) {
			registerJDBC(urlOrDriver);
		}
		return dbu;
	}

	public DBUtility addTypeConverter(Class clazz, DBTypeConverter converter) {
		converters.put(clazz, converter);
		return this;
	}

	public Map<Class, DBTypeConverter> getTypeConverters() {
		return converters;
	}

	public Map<Class, SQLType> getCustomSqlTypeMap() {
		return customSqlTypeMap;
	}

	public boolean isFixAutoCommitIssue() {
		return fixAutoCommitIssue;
	}

	public DBUtility setFixAutoCommitIssue(boolean fixAutoCommitIssue) {
		this.fixAutoCommitIssue = fixAutoCommitIssue;
		return this;
	}

	public DBUtility setCustomSqlTypeMap(Map<Class, SQLType> customSqlTypeMap) {
		if (customSqlTypeMap == null || customSqlTypeMap.isEmpty()) {
			throw new IllegalArgumentException("SQL map cannot be empty or null");
		}
		this.customSqlTypeMap = customSqlTypeMap;
		return this;
	}

	public String getUrl() {
		return url;
	}

	public DBUtility setUrl(String url) {
		if (url == null || url.trim().isEmpty()) {
			throw new IllegalArgumentException("URL cannot be empty or null");
		}
		this.url = url;
		return this;
	}

	public DBUtility setCredentials(String login, String password) {
		this.login = login;
		this.password = password;
		return this;
	}

	public ArrayList<HashMap<String, Object>> executeQuery(String query, Object... params) throws
		SQLException {
		DBParameter[] dbParams = new DBParameter[params == null ? 0 : params.length];
		if (params != null) {
			int i = 0;
			for (Object p : params) {
				dbParams[i++] = new DBParameter(p);
			}
		}
		try (Connection con = DriverManager.getConnection(url, login, password)) {
			return execute(con, false, query, dbParams);
		}
	}

	public ArrayList<HashMap<String, Object>> executeCall(String query, DBParameter... params)
		throws SQLException {
		try (Connection con = DriverManager.getConnection(url, login, password)) {
			return execute(con, true, query, params);
		}
	}

	public ArrayList<HashMap<String, Object>> execute(Connection con, boolean isCallable,
													  String query, DBParameter... params) throws
		SQLException {
		int i = 1;
		ArrayList<HashMap<String, Object>> values = new ArrayList<>();
		ArrayList<Integer> outParamIndexes = new ArrayList<>();
		if (fixAutoCommitIssue) {
			con.setAutoCommit(false);
		}
		try (PreparedStatement statement = isCallable ? con.prepareCall(query) : con.
			prepareStatement(query)) {
			if (params != null) {
				HashMap<DBParameter, Integer> sqlParams = getSqlTypes(params);
				for (Map.Entry<DBParameter, Integer> p : sqlParams.entrySet()) {
					DBParameter param = p.getKey();
					int sqlType = p.getValue();
					Object data = param.getLeft();
					if (param.isOut()) {
						outParamIndexes.add(i - 1);
						((CallableStatement) statement).registerOutParameter(i++, sqlType);
					} else {
						if (data == null) {
							statement.setNull(i++, Types.NULL); //TODO: check
						} else {
							statement.setObject(i++, data, sqlType);
						}
					}
				}
			}
			if (statement.execute()) {
				try (ResultSet rs = statement.getResultSet()) {
					values = convertResultSetToHashMaps(rs, converters);
				}
			}
			for (int index : outParamIndexes) {
				params[index].setData(((CallableStatement) statement).getObject(index + 1));
			}
		}
		if (fixAutoCommitIssue) {
			try {
				con.commit();
			} catch (SQLException ex) {
				LOG.warning(ownStack(ex));
			}
		}
		return values;
	}

	public static ArrayList<HashMap<String, Object>> convertResultSetToHashMaps(ResultSet rs,
																				Map<Class, DBTypeConverter> converters)
		throws SQLException {
		ArrayList<HashMap<String, Object>> values = new ArrayList<>();
		if (rs != null) {
			ResultSetMetaData metaData = rs.getMetaData();
			while (rs.next()) {
				HashMap<String, Object> record = new HashMap<>();
				for (int i = 1; i <= metaData.getColumnCount(); ++i) {
					String name = metaData.getColumnName(i);
					Object data = rs.getObject(i);
					if (data != null && converters != null) {
						DBTypeConverter converter = converters.get(data.getClass());
						if (converter == null) {
							for (Entry<Class, DBTypeConverter> entry : converters.entrySet()) {
								if (entry.getKey().isInstance(data)) {
									data = entry.getValue().convert(data, name);
								}
							}
						}
						else{
							data = converter.convert(data, name);
						}
					}
					record.put(name, data);
				}
				values.add(record);
			}
		}
		return values;
	}

	/**
	 * Регистрация класса JDBC драйвера в RunTime.
	 * В случае если драйвер уже зарегистрирован - ничего не происходит.
	 * Это костыльный метод, поскольку Classloader не всегда сразу подгружает необходимый драйвер
	 * для подключения
	 *
	 * @param urlOrDriver Название класса драйвера или JDBC URL.
	 *
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	private static void registerJDBC(String urlOrDriver) throws SQLException,
																ClassNotFoundException{
		Enumeration<Driver> drivers = DriverManager.getDrivers();
		while (drivers.hasMoreElements()) {
			Driver driver = drivers.nextElement();
			if (driver.acceptsURL(urlOrDriver)
				|| urlOrDriver.equalsIgnoreCase(driver.getClass().getCanonicalName())) {
				return;
			}
		}
		try {
			Driver loadDriver = (Driver) Class.forName(urlOrDriver).newInstance();
			DriverManager.registerDriver(loadDriver);
		} catch (InstantiationException | IllegalAccessException ex) {
			LOG.log(Level.SEVERE, ownStack(ex));
		}
		
	}

	private HashMap<DBParameter, Integer> getSqlTypes(DBParameter[] params) {
		HashMap<DBParameter, Integer> out = new HashMap<>();
		int i = 0;
		for (DBParameter p : params) {
			Object param = p.getLeft();
			Integer type = p.isOut() ? Types.OTHER : customSqlTypeMap.get(param.getClass()).
				getVendorTypeNumber();
			if (type == null) {
				throw new IllegalArgumentException(
					"Unknown SQL type for parameter " + i + "\nType Map: " + customSqlTypeMap);
			}
			out.put(p, type);
			++i;
		}
		return out;
	}

}
