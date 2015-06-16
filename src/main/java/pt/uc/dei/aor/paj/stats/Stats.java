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
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;


public class Stats implements MessageListener {
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
			// TODO Logger validation
			return;
		} catch (Exception e) {
			// TODO Logger generate file
			return;
		}
		
		try {
			String xml = tmsg.getText();
			Pattern p = Pattern.compile("<date>(\\d{4})\\-(\\d{1,2})\\-(\\d{1,2})T(\\d{1,2}):(\\d{1,2}):\\d{1,2}\\.\\d{1,3}Z</date>");
			Matcher m = p.matcher(xml);
			
			int counter = 0;
			while (m.find()) {
				System.out.println(m.group());
				int year = Integer.parseInt(m.group(1));
				int month = (Integer.parseInt(m.group(2))-1)%12;
				int day = Integer.parseInt(m.group(3));
				int hour = Integer.parseInt(m.group(4));
				int minute = Integer.parseInt(m.group(5));
				Calendar c = new GregorianCalendar(year, month, day, hour, minute);
				c.setTimeZone(TimeZone.getTimeZone("GMT"));
				Calendar nowMinus12h = new GregorianCalendar();
				nowMinus12h.add(Calendar.HOUR_OF_DAY, -12);
				System.out.println(c.getTime());
				System.out.println(nowMinus12h.getTime());
				System.out.println(c.after(nowMinus12h));
				if (c.after(nowMinus12h)) counter++;
			}
			writeFIleStats("Número de noticias nas ultimas 12 horas: " +counter+" noticias.");
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Logger função writeFileStats escreve ou não
			e.printStackTrace();
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
			jcontext.setClientID("parvo");
			JMSConsumer consumer = jcontext.createDurableConsumer(topic, "parvo");
			consumer.setMessageListener(this);
			Thread.sleep(5000);
		} catch (JMSRuntimeException | InterruptedException re) {
			re.printStackTrace();
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