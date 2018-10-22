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
package tk.sot_tech.dbexecutor;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import static tk.sot_tech.misc.Misc.ownStack;
import tk.sot_tech.misc.Pair;

/**
 *
 * @author sot
 */
public final class DBUtility {

	private final HashMap<Class, DBTypeConverter> converters = new HashMap<>(DEFAULT_CONVERTERS);
	public static final Map<Class, DBTypeConverter> DEFAULT_CONVERTERS;
	private String url, login, password;
	private final HashMap<Class, Integer> customSqlTypeMap = new HashMap<>(DEFAULT_SQL_TYPE_MAP);
	public static final Map<Class, Integer> DEFAULT_SQL_TYPE_MAP;
	private boolean fixAutoCommitIssue = false, setNullAsSubstitution = false;
	private static final Logger LOG = Logger.getLogger(DBUtility.class.getName());
	private static final DBTypeConverter RESULT_SET_CONVERTER = (Object data, String columnName) -> {
		try {
			return convertResultSetToHashMaps((ResultSet) data, null);
		} catch (SQLException ex) {
			LOG.log(Level.SEVERE, ownStack(ex));
			return new ArrayList<HashMap<String, Object>>();
		}
	};

	static {
		HashMap<Class, Integer> tmp = new HashMap<>();
		tmp.put(String.class, Types.VARCHAR);
		tmp.put(BigDecimal.class, Types.NUMERIC);
		tmp.put(Boolean.class, Types.BIT);
		tmp.put(Byte.class, Types.TINYINT);
		tmp.put(Short.class, Types.SMALLINT);
		tmp.put(Integer.class, Types.INTEGER);
		tmp.put(Long.class, Types.BIGINT);
		tmp.put(Float.class, Types.REAL);
		tmp.put(Double.class, Types.DOUBLE);
		tmp.put(byte[].class, Types.VARBINARY);
		tmp.put(Date.class, Types.DATE);
		tmp.put(Time.class, Types.TIME);
		tmp.put(Timestamp.class, Types.TIMESTAMP);

		DEFAULT_SQL_TYPE_MAP = Collections.unmodifiableMap(tmp);

		HashMap<Class, DBTypeConverter> tmp0 = new HashMap<>();
		tmp0.put(ResultSet.class, RESULT_SET_CONVERTER);
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

	public DBUtility setTypeConverter(Class clazz, DBTypeConverter converter) {
		converters.put(clazz, converter);
		return this;
	}

	public Map<Class, DBTypeConverter> getTypeConverters() {
		return converters;
	}

	public Map<Class, Integer> getCustomSqlTypeMap() {
		return customSqlTypeMap;
	}

	public DBUtility setCustomSqlType(Class clazz, int sqlType) {
		customSqlTypeMap.put(clazz, sqlType);
		return this;
	}

	public boolean isFixAutoCommitIssue() {
		return fixAutoCommitIssue;
	}

	public DBUtility setFixAutoCommitIssue(boolean fixAutoCommitIssue) {
		this.fixAutoCommitIssue = fixAutoCommitIssue;
		return this;
	}

	public boolean isSetNullAsSubstitution() {
		return setNullAsSubstitution;
	}

	public DBUtility setSetNullAsSubstitution(boolean setNullAsSubstitution) {
		this.setNullAsSubstitution = setNullAsSubstitution;
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
		PreparedStatement statement = null;
		try {
			ArrayList<Pair<DBParameter, Integer>> sqlParams = getSqlTypes(params);
			String preparedQuery = setNullAsSubstitution ? replaceNulls(query, sqlParams) : query;
			statement = isCallable ? con.prepareCall(preparedQuery)
						: con.prepareStatement(preparedQuery);

			for (Pair<DBParameter, Integer> p : sqlParams) {
				DBParameter param = p.getLeft();
				int sqlType = p.getRight();
				Object data = param.getData();
				if (param.isOut()) {
					outParamIndexes.add(i - 1);
					((CallableStatement) statement).registerOutParameter(i++, sqlType);
				} else {
					if (data == null) {
						statement.setNull(i++, Types.NULL);
					} else {
						if (sqlType == Types.OTHER) {
							statement.setObject(i++, data);
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
				params[index].setData(convertObject(((CallableStatement) statement).getObject(index + 1), null, converters));
			}
		} finally {
			if (statement != null) {
				statement.close();
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
					Object data = convertObject(rs.getObject(i), name, converters);
					record.put(name, data);
				}
				values.add(record);
			}
		}
		return values;
	}

	public static Object convertObject(Object in, String name, Map<Class, DBTypeConverter> converters) {
		Object out = null;
		if (in != null && converters != null) {
			DBTypeConverter converter = converters.get(in.getClass());
			if (converter == null) {
				for (Entry<Class, DBTypeConverter> entry : converters.entrySet()) {
					if (entry.getKey().isInstance(in)) {
						out = entry.getValue().convert(in, name);
					}
				}
			} else {
				out = converter.convert(in, name);
			}
		}
		return out;
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
																ClassNotFoundException {
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

	private String replaceNulls(String query, ArrayList<Pair<DBParameter, Integer>> sqlParams) {
		ArrayList<Pair<DBParameter, Integer>> trimmedParams = new ArrayList<>();
		String newQuery = query;
		int index = 0;
		for (Pair<DBParameter, Integer> param : sqlParams) {
			index = newQuery.indexOf('?', index);
			if (param.getRight() != Types.NULL) {
				trimmedParams.add(param);
			} else {

				int maxIndex = newQuery.length() - 1;
				if (index >= 0) {
					newQuery = newQuery.substring(0, index) + " NULL "
							   + (index == maxIndex ? "" : newQuery.substring(index + 1, newQuery.
																			  length()));
				}
			}
			++index;
		}
		sqlParams.clear();
		sqlParams.addAll(trimmedParams);
		return newQuery;
	}

	private ArrayList<Pair<DBParameter, Integer>> getSqlTypes(DBParameter[] params) {
		ArrayList<Pair<DBParameter, Integer>> out = new ArrayList<>();
		if (params != null) {
			for (DBParameter p : params) {
				Object param = p.getLeft();
				Integer type;
				if (p.isOut()) {
					type = Types.OTHER;
				} else {
					if (param == null) {
						type = Types.NULL;
					} else {
						Class clazz = param.getClass();
						if (customSqlTypeMap.containsKey(clazz)) {
							type = customSqlTypeMap.get(clazz);
						} else {
							LOG.log(Level.WARNING,
									"Unable to find SQL type mapping for class {0} falling back to Types.OTHER",
									clazz.getName());
							type = Types.OTHER;
						}
					}
				}
				Pair<DBParameter, Integer> pair = new Pair<>(p, type);
				out.add(pair);
			}
		}
		return out;
	}

}
