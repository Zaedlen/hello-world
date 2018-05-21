
package interfaz;

import java.sql.*;

/**
 * Clase principal del codigo de negocio. Contiene tanto el codigo de cada una
 * de las 21 consultas como los enunciados. Cuando se instancia una Consultas,
 * se crean el Connection y el Statement necesarios y, cada entero dado al metodo
 * consultar() equivale a una de las consultas de la practica.
 * @author Daniel
 */
public class Consultas {
    private Connection c;
    private Statement s;
    
    /**
     * Establecimiento de la conexion con la BDD
     * @param url
     * @param user
     * @param pass
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException 
     */
    public Consultas(String url, String user, String pass) throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException{
        Class.forName("org.postgresql.Driver").newInstance();
        c = DriverManager.getConnection(url, user, pass);
        System.out.println("La conexion ha tenido exito");
        
        s = c.createStatement();
    }
    
    /**
     * Desconexion de la BDD y cierre de los canales.
     * @throws SQLException 
     */
    public void desconectar() throws SQLException{
        s.close();
        c.close();
        System.out.println("La desconexion ha tenido exito");
    }
    
    /**
     * Realiza las consultas y las devuelve con formato para presentarlas en
     * forma de tabla
     * @param consulta
     * @return
     * @throws SQLException 
     */
    public String consultar(int consulta) throws SQLException{
        ResultSet r = getConsulta(consulta);
        String enun = getEnunciado(consulta);
        String tabla = getTabla(r, enun);
        r.close();
        return tabla;
    }
    
    private String getEnunciado(int consulta){
        String enun = null;
        switch(consulta){
            case 1:
                enun = "Mostrar los artículos presentes en la base de datos,"+
                        " mostrando su código y el\nprecio de venta.";
                break;
            case 2:
                enun = "Determinar el número total de surtidores que hay en la gasolinera.";
                break;
            case 3:
                enun = "Obtener el total del dinero facturado por las tiendas de la gasolinera desde la\n" +
                        "implementación de la base de datos.";
                break;
            case 4:
                enun = "Mostrar el nombre de los empleados de las tiendas que trabajan en turno de\n" +
                        "mañana.";
                break;
            case 5:
                enun = "Mostrar los puntos totales canjeados por los socios de la gasolinera, junto\n" +
                        "con su identificador y nombre.";
                break;
            case 6:
                enun = "Mostrar los 5 artículos más canjeados por los socios.";
                break;
            case 7:
                enun = "Mostrar el importe total devuelto en los tickets en todos los sorteos\n" +
                        "realizados hasta la fecha.";
                break;
            case 8:
                enun = "Determinar el grado de satisfacción medio de las opiniones que los clientes\n" +
                       "han realizado por internet, mostrando la puntuación media.";
                break;
            case 9:
                enun = "Determinar el número de tickets que ha emitido cada tienda, mostrando el\n" +
                        "número de tickets y el nombre de la tienda. Ordenar la salida de mayor a\n" +
                        "menor.";
                break;
            case 10:
                enun = "Aunque el sorteo de tickets se hace aleatoriamente entre todos los emitidos\n" +
                        "en la semana, determinar cuál es la tienda en la que más tickets han sido\n" +
                        "premiados hasta la fecha.";
                break;
            case 11:
                enun = "De los repostajes que proporcionan puntos a los socios, determinar el total de\n" +
                        "litros repostados en cada surtidor, ordenando la salida de menor a mayor.";
                break;
            case 12:
                enun = "Mostrar el tamaño medio y el precio/litro de los surtidores de gasolina y\n" +
                        "gasoil.";
                break;
            case 13:
                enun = "Determinar el número trabajadores que tiene cada tienda y cada surtidor,\n" +
                        "ordenando la salida de menor a mayor.";
                break;
            case 14:
                enun = "Mostrar el número de surtidores de gasolina, gasoil, GLP e Hidrógeno que\n" +
                        "hay en la gasolinera, ordenado de mayor a menor.";
                break;
            case 15:
                enun = "Mostrar el porcentaje de surtidores de gasolina, gasoil, GLP e Hidrógeno que\n" +
                        "existen en la gasolinera.";
                break;
            case 16:
                enun = "Mostrar el valor total de todos los artículos canjeados por los socios en el\n" +
                        "último mes.";
                break;
            case 17:
                enun = "Mostrar las fechas en las que no se puede utilizar los surtidores que se\n" +
                        "encuentren averiados.";
                break;
            case 18:
                enun = "Mostrar a los supervisores (DNI y nombre) de los empleados que trabajan en\n" +
                        "surtidores de GLP, en los que los socios hayan repostado y, posteriormente,\n" +
                        "canjeado artículos.";
                break;
            case 19:
                enun = "Mostrar los artículos que han sido canjeados por los socios y que hayan sido\n" +
                        "comprados en las tiendas.";
                break;
            case 20:
                enun = "Mostrar los artículos que ni hayan sido canjeados por los socios ni que hayan\n" +
                        "sido comprados en las tiendas.";
                break;
            case 21:
                enun = "Mostrar los artículos que hayan sido canjeados por los socios y que hayan\n" +
                        "sido comprados en las tiendas, pero que no aparezcan en tickets premiados.";
                break;
        }
        return enun;
    }
    
    private ResultSet getConsulta(int consulta) throws SQLException{
        String query = null;
        switch(consulta){
            case 1:
                query = "SELECT codigo, precio FROM articulo";
                break;
            case 2:
                query = "SELECT count (DISTINCT num) AS total FROM surtidor";
                break;
            case 3:
                query = "SELECT sum(cuantia) AS total FROM ticket";
                break;
            case 4:
                query = "SELECT nombre\n" +
                        "FROM empleado\n" +
                        "WHERE turno_tie='mañana'";
                break;
            case 5:
                query = "SELECT canjea.nick, cliente.dni, cliente.nombre, sum(canjea.ptos) AS \"ptos canjeados\"\n" +
                        "FROM canjea, cliente\n" +
                        "WHERE canjea.nick=cliente.nick\n" +
                        "GROUP BY canjea.nick, cliente.dni, cliente.nombre";
                break;
            case 6:
                query = "SELECT codigo_a, total\n" +
                        "FROM (SELECT codigo_a, count(codigo_a) AS total\n" +
                        "	FROM canjea\n" +
                        "	GROUP BY codigo_a\n" +
                        "	ORDER BY total desc limit 5)AS ranking";
                break;
            case 7:
                query = "SELECT sum(cuantia_iva*0.5)total\n" +
                        "FROM ticket\n" +
                        "WHERE fecha_premio is not null";
                break;
            case 8:
                query = "SELECT round(avg(opinion.puntuacion),2) AS media\n" +
                        "FROM opinion";
                break;
            case 9:
                query = "SELECT tienda_emite, count(codigo) AS total\n" +
                        "FROM ticket\n" +
                        "GROUP BY tienda_emite\n" +
                        "ORDER BY total desc";
                break;
            case 10:
                query = "SELECT premios.tienda_emite, premios.total\n" +
                        "FROM (SELECT tienda_emite, count(tienda_emite) AS total\n" +
                        "	FROM ticket\n" +
                        "	WHERE fecha_premio is not null\n" +
                        "	GROUP BY tienda_emite\n" +
                        "	ORDER BY total desc) AS premios\n" +
                        "LIMIT 1";
                break;
            case 11:
                query = "SELECT num_s, sum(litros) AS total\n" +
                        "FROM reposta\n" +
                        "GROUP BY num_s\n" +
                        "ORDER BY total asc";
                break;
            case 12:
                query = "(SELECT 'gasolina' AS tipo,\n" +
                        "	round(avg(surtidor.precio_litro),2) AS \"precioMedio\",\n" +
                        "	round(avg(surtidor.cap_tanque),2) AS \"capMedia\"\n" +
                        "FROM surtidor, gasolina\n" +
                        "WHERE gasolina.num = surtidor.num)\n" +
                        "\n" +
                        "UNION\n" +
                        "\n" +
                        "(SELECT 'gasoleo' AS tipo,\n" +
                        "	round(avg(surtidor.precio_litro),2) AS \"precioMedio\",\n" +
                        "	round(avg(surtidor.cap_tanque),2) AS \"capMedia\"\n" +
                        "FROM surtidor, gasoleo\n" +
                        "WHERE gasoleo.num = surtidor.num)";
                break;
            case 13:
                query = "(SELECT 'tienda' AS tipo,\n" +
                        "	nombre_tie AS id,\n" +
                        "	count(nombre_tie) AS trabajadores\n" +
                        "FROM empleado\n" +
                        "GROUP BY nombre_tie)\n" +
                        "\n" +
                        "UNION\n" +
                        "\n" +
                        "(SELECT 'surtidor' AS tipo,\n" +
                        "	CAST(num AS varchar)AS \"id\",\n" +
                        "	count(num) AS trabajadores\n" +
                        "FROM trabaja_surt\n" +
                        "GROUP BY num)\n" +
                        "ORDER BY trabajadores asc";
                break;
            case 14:
                query = "(SELECT 'gasolina' AS tipo,\n" +
                        "	count(*) AS total\n" +
                        "FROM gasolina)\n" +
                        "\n" +
                        "UNION\n" +
                        "\n" +
                        "(SELECT 'gasoleo' AS tipo,\n" +
                        "	count(*) AS total\n" +
                        "FROM gasoleo)\n" +
                        "\n" +
                        "UNION\n" +
                        "\n" +
                        "(SELECT 'glp' AS tipo,\n" +
                        "	count(*) AS total\n" +
                        "FROM glp)\n" +
                        "\n" +
                        "UNION\n" +
                        "\n" +
                        "(SELECT 'hidrogeno' AS tipo,\n" +
                        "	count(*) AS total\n" +
                        "FROM hidrogeno)\n" +
                        "\n" +
                        "ORDER BY total desc";
                break;
            case 15:
                query = "SELECT tipo, round(total*100.00/(SELECT count(*) FROM surtidor),2) AS porcentaje\n" +
                        "FROM \n" +
                        "((SELECT 'gasolina' AS tipo,\n" +
                        "	count(*) AS total\n" +
                        "FROM gasolina)\n" +
                        "\n" +
                        "UNION\n" +
                        "\n" +
                        "(SELECT 'gasoleo' AS tipo,\n" +
                        "	count(*) AS total\n" +
                        "FROM gasoleo)\n" +
                        "\n" +
                        "UNION\n" +
                        "\n" +
                        "(SELECT 'glp' AS tipo,\n" +
                        "	count(*) AS total\n" +
                        "FROM glp)\n" +
                        "\n" +
                        "UNION\n" +
                        "\n" +
                        "(SELECT 'hidrogeno' AS tipo,\n" +
                        "	count(*) AS total\n" +
                        "FROM hidrogeno)\n" +
                        "\n" +
                        "ORDER BY total desc) AS ranking";
                break;
            case 16:
                query = "SELECT round(sum(precio),2) AS total\n" +
                        "FROM (SELECT canjea.fecha,\n" +
                        "	sum(articulo.precio)*1.21 AS precio\n" +
                        "	FROM canjea, articulo\n" +
                        "	WHERE (canjea.codigo_a=articulo.codigo)\n" +
                        "	AND ((current_date-canjea.fecha)<=30)\n" +
                        "	GROUP BY canjea.fecha) AS factura";
                break;
            case 17:
                query = "SELECT num AS \"surtidor id\",\n" +
                        "	(CAST('Del '||current_date AS varchar)||\n" +
                        "		' al '||\n" +
                        "		CAST(fech_averia AS varchar))\n" +
                        "		AS \"Intervalo reparaciones\"\n" +
                        "FROM surtidor\n" +
                        "WHERE (fech_averia is not null) AND (fech_averia>current_date)";
                break;
            case 18:
                query = "SELECT dni AS \"dni jefe\", nombre AS \"nombre jefe\"\n" +
                        "FROM empleado\n" +
                        "WHERE dni=ANY(SELECT jefe_dni\n" +
                        "		FROM glp, surtidor, trabaja_surt, empleado, reposta, cliente, canjea\n" +
                        "		WHERE (glp.num=surtidor.num) AND\n" +
                        "		      (surtidor.num=trabaja_surt.num) AND\n" +
                        "		      (trabaja_surt.dni = empleado.dni) AND\n" +
                        "		      (surtidor.num=reposta.num_s) AND\n" +
                        "		      (reposta.nick=cliente.nick) AND\n" +
                        "		      (cliente.nick=canjea.nick))";
                break;
            case 19:
                query = "SELECT DISTINCT articulo.codigo, articulo.puntos, articulo.precio\n" +
                        "FROM articulo, canjea, contiene\n" +
                        "WHERE (articulo.codigo=canjea.codigo_a) AND\n" +
                        "	(articulo.codigo=contiene.codigo_a)";
                break;
            case 20:
                query = "SELECT *\n" +
                        "FROM articulo\n" +
                        "WHERE codigo NOT IN \n" +
                        "	(SELECT DISTINCT articulo.codigo\n" +
                        "	FROM articulo, canjea, contiene\n" +
                        "	WHERE (articulo.codigo=canjea.codigo_a) AND\n" +
                        "		(articulo.codigo=contiene.codigo_a))";
                break;
            case 21:
                query = "SELECT *\n" +
                        "FROM articulo\n" +
                        "WHERE (codigo IN (SELECT DISTINCT articulo.codigo\n" +
                        "			FROM articulo, canjea, contiene\n" +
                        "			WHERE (articulo.codigo=canjea.codigo_a) AND\n" +
                        "			(articulo.codigo=contiene.codigo_a)))\n" +
                        "	AND (codigo NOT IN(SELECT articulo.codigo\n" +
                        "				FROM articulo, contiene, ticket\n" +
                        "				WHERE (articulo.codigo=contiene.codigo_a) AND\n" +
                        "					(contiene.codigo_t=ticket.codigo) AND\n" +
                        "					(ticket.fecha_premio is not null)))";
                break;
        }
        
        ResultSet r = s.executeQuery(query);
        return r;
    }
    
    private String getTabla(ResultSet r, String enunciado) throws SQLException{
        int ancho = 20;
        ResultSetMetaData md = r.getMetaData();
        String tabla = enunciado+"\n\n";
        String format;
        boolean next = r.next();
        for(int i=1;i<=md.getColumnCount();i++){//Obtenemos el ancho de columna maximo
            if(md.getColumnLabel(i).length()>ancho) ancho = md.getColumnLabel(i).length();
            if((next)&&(r.getString(i).length()>ancho)) ancho = r.getString(i).length();
        }
        format = "%"+ancho+"s";
        for(int i=1;i<=md.getColumnCount();i++){
            tabla += String.format(format, md.getColumnLabel(i))+"|";
        }
        tabla+= "\n";
        for(int i=0;i<(md.getColumnCount()*ancho+md.getColumnCount());i++) tabla+="-";
        tabla+= "\n";
        while(next){
            for(int i=1;i<=md.getColumnCount();i++){
                tabla += String.format(format, r.getString(i))+"|";
            }
            tabla += "\n";
            next = r.next();
        }
        tabla +="\n";
        
        return tabla;
    }
}
