package pt.uc.dei.aor.paj.stats;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jms.ConnectionFactory;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;



public class Stats implements MessageListener {
	private static final Logger logger = LogManager.getLogger(Stats.class);
	private ConnectionFactory cf;
	private Topic topic;

	public Stats() throws NamingException {
		this.cf = InitialContext.doLookup("jms/RemoteConnectionFactory");
		topic = InitialContext.doLookup("jms/topic/testTopic");
	}

	@Override
	public void onMessage(Message msg) {
		TextMessage tmsg = (TextMessage) msg;
		try {
			createXML(tmsg.getText());
			validateXml();
		} catch (SAXException | IOException e1) {
			logger.error("XML inválido. "+e1.getMessage());
			return;
		} catch (Exception e) {
			logger.error("Não é possível criar ficheiro XML.");
			return;
		}
		
		try {
			String xml = tmsg.getText();
			Pattern p = Pattern.compile("<date>(\\d{4})\\-(\\d{1,2})\\-(\\d{1,2})T(\\d{1,2}):(\\d{1,2}):\\d{1,2}Z</date>");
			Matcher m = p.matcher(xml);
			
			int counter = 0;
			while (m.find()) {
				int year = Integer.parseInt(m.group(1));
				int month = (Integer.parseInt(m.group(2))-1)%12;
				int day = Integer.parseInt(m.group(3));
				int hour = Integer.parseInt(m.group(4));
				int minute = Integer.parseInt(m.group(5));
				Calendar c = new GregorianCalendar(year, month, day, hour, minute);
				c.setTimeZone(TimeZone.getTimeZone("GMT"));
				Calendar nowMinus12h = new GregorianCalendar();
				nowMinus12h.add(Calendar.HOUR_OF_DAY, -12);
				if (c.after(nowMinus12h)) counter++;
			}
			
			int counterTotal = 0;
			p = Pattern.compile("<noticia>");
			m = p.matcher(xml);
			while (m.find()) {
				counterTotal++;
			}
			writeFIleStats("Número total de notícias: " +counterTotal+" notícias.\n"
					+ "Número de notícias nas últimas 12 horas: " +counter+" notícias.");
		} catch (JMSException e) {
			logger.error("Erro de acesso JMS");
		} catch (IOException e) {
			logger.error("Não é possível criar ficheiro XML.");
		}
		
	}
	
	
	private void createXML(String msg) throws Exception{
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.parse(new InputSource(new StringReader(msg)));
		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = transformerFactory.newTransformer();
		DOMSource source = new DOMSource(doc);
		StreamResult result =  new StreamResult(new File("jornal.xml"));
		transformer.transform(source, result);
	}
	
	private static void validateXml() throws SAXException, IOException {
		SchemaFactory factory = 
					SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
			Schema schema = factory.newSchema(new File("jornal.xsd"));
			Validator validator = schema.newValidator();
			validator.validate(new StreamSource(new File("jornal.xml")));
	}

	public void launch_and_wait() {
		try (JMSContext jcontext = cf.createContext("mr", "mr2015");) {
			jcontext.setClientID("statsId");
			JMSConsumer consumer = jcontext.createDurableConsumer(topic, "statsId");
			consumer.setMessageListener(this);
			Thread.sleep(5000);
		} catch (JMSRuntimeException | InterruptedException re) {
			logger.error("Erro acesso JMS");
		}
	}
	
	public void writeFIleStats(String stats) throws IOException{
		BufferedWriter out=new BufferedWriter(new FileWriter("stat.txt"));
		out.write(stats);
		out.newLine();
		out.close();
		
	}

	public static void main(String[] args) throws NamingException {
		Stats r = new Stats();
		r.launch_and_wait();
	}


}