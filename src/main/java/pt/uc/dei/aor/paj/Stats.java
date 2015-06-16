package pt.uc.dei.aor.paj;
import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
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
			String xml = tmsg.getText();
			//System.out.println(xml);
			Pattern p = Pattern.compile("<date>(\\d{4})\\-(\\d{1,2})\\-(\\d{1,2})T(\\d{1,2}):(\\d{1,2}):(\\d{1,2})\\d{1,2}\\.\\d{1,3}Z</date>");
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
			System.out.println(counter);
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void launch_and_wait() {
		try (JMSContext jcontext = cf.createContext("mr", "mr2015");) {
			jcontext.setClientID("parvo");
			JMSConsumer consumer = jcontext.createDurableConsumer(topic, "parvo");
			consumer.setMessageListener(this);
//			System.out.println("Press enter to finish...");
//			System.in.read();
			Thread.sleep(5000);
		} catch (JMSRuntimeException | InterruptedException re) {
			re.printStackTrace();
		}
	}

	public static void main(String[] args) throws NamingException {
		Stats r = new Stats();
		r.launch_and_wait();
	}



}