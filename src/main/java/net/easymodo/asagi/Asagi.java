package net.easymodo.asagi;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import net.easymodo.asagi.exception.BoardInitException;
import net.easymodo.asagi.settings.BoardSettings;
import net.easymodo.asagi.settings.OuterSettings;
import net.easymodo.asagi.settings.RedisCacheSettings;
import net.easymodo.asagi.settings.Settings;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class Asagi {
    private static final String SETTINGS_FILE = "./asagi.json";
    private static final String DEBUG_FILE = "./debug.log";

    private static BufferedWriter debugOut = null;
    private static String dumperEngine;
    private static String sourceEngine;

    private static Map<String, Class<? extends DB>> dbClassMapper = new HashMap<String, Class<? extends DB>>();
    static {
        dbClassMapper.put("Mysql", Mysql.class);
        dbClassMapper.put("Pgsql", Pgsql.class);
    }

    private static Map<String, Class<? extends YotsubaAbstract>> sourceBoardMapper = new HashMap<String, Class<? extends YotsubaAbstract>>();
    static {
        sourceBoardMapper.put("YotsubaHTML", YotsubaHTML.class);
        sourceBoardMapper.put("YotsubaJSON", YotsubaJSON.class);
    }

    private static Map<String, Class<? extends AbstractDumper>> dumperMapper = new HashMap<String, Class<? extends AbstractDumper>>();
    static {
        dumperMapper.put("DumperClassic", DumperClassic.class);
        dumperMapper.put("DumperJSON", DumperJSON.class);
    }

    public static BufferedWriter getDebugOut() {
        return debugOut;
    }

    private static BoardSettings getBoardSettings(Settings settings, String boardName) {
        BoardSettings defaults = settings.getBoardSettings().get("default");
        BoardSettings bSet = settings.getBoardSettings().get(boardName);

        bSet.initSetting("path", defaults.getPath() + "/" + boardName + "/");
        bSet.initSetting("table", boardName);

        // set everything that isn't set already to their defaults
        bSet.initSettings(defaults);

        return bSet;
    }

    private static void spawnBoard(String boardName, Settings settings) throws BoardInitException {
        BoardSettings bSet = getBoardSettings(settings, boardName);
        RedisCacheSettings redisCacheSettings = settings.getRedisCacheSettings();

        final RedisCache redisCache;
        if (redisCacheSettings != null) {
            redisCache = new RedisCache(redisCacheSettings, bSet);
        } else {
            redisCache = null;
        }

        int pageLimbo = bSet.getDeletedThreadsThresholdPage();
        boolean fullThumb = (bSet.getThumbThreads() != 0);
        boolean fullMedia = (bSet.getMediaThreads() != 0);

        // Init source board engine through reflection
        Board sourceBoard;
        try {
            Class<? extends YotsubaAbstract> sourceBoardClass = sourceBoardMapper.get(sourceEngine);
            sourceBoard = sourceBoardClass.getConstructor(String.class, BoardSettings.class).newInstance(boardName, bSet);
        } catch(Exception e) {
            throw new BoardInitException("Error initializing board engine " + sourceEngine);
        }

        // Same for DB engine
        String boardEngine = bSet.getEngine() == null ? "Mysql" : bSet.getEngine();
        bSet.setEngine(boardEngine);

        Class<? extends DB> sqlBoardClass;
        Constructor<? extends DB> boardCnst;

        // Init two DB objects: one for topic insertion and another
        // for media insertion
        DB topicDb;
        DB mediaDb;

        try {
            sqlBoardClass = dbClassMapper.get(boardEngine);
            boardCnst = sqlBoardClass.getConstructor(String.class, BoardSettings.class);

            // For topics
            topicDb = boardCnst.newInstance(bSet.getPath(), bSet);

            // For media
            mediaDb = boardCnst.newInstance(bSet.getPath(), bSet);
        } catch(NoSuchMethodException e) {
            throw new BoardInitException("Error initializing board engine " + boardEngine);
        } catch(InstantiationException e) {
            throw new BoardInitException("Error initializing board engine " + boardEngine);
        } catch(IllegalAccessException e) {
            throw new BoardInitException("Error initializing board engine " + boardEngine);
        } catch(InvocationTargetException e) {
            if(e.getCause() instanceof BoardInitException)
                throw (BoardInitException)e.getCause();
            else if(e.getCause() instanceof RuntimeException)
                throw (RuntimeException)e.getCause();
            throw new BoardInitException("Error initializing board engine " + boardEngine);
        }

        Local topicLocalBoard = new Local(bSet.getPath(), bSet, topicDb);
        Local mediaLocalBoard = new Local(bSet.getPath(), bSet, mediaDb);

        // And the dumper, le sigh.
        AbstractDumper dumper;
        try {
            Class<? extends AbstractDumper> dumperClass = dumperMapper.get(dumperEngine);
            dumper = dumperClass.getConstructor(String.class, Local.class, Local.class, Board.class, boolean.class, boolean.class, int.class)
                    .newInstance(boardName, topicLocalBoard, mediaLocalBoard, sourceBoard, fullThumb, fullMedia, pageLimbo);
        } catch(Exception e) {
            throw new BoardInitException("Error initializing dumper engine " + dumperEngine);
        }
        dumper.initDumper(bSet);
    }

    public static void main(String[] args) {
        Settings fullSettings;
        String settingsJson;
        Gson gson = new Gson();
        String settingsFileName = SETTINGS_FILE;

        for(int i = 0; i < args.length; ++i) {
            if(args[i].equals("--config") && ++i < args.length) {
                settingsFileName = args[i];
            }
        }

        File debugFile = new File(DEBUG_FILE);
        try {
            debugOut = new BufferedWriter(Files.newWriterSupplier(debugFile, Charsets.UTF_8, true).getOutput());
        } catch(IOException e1) {
            System.err.println("WARN: Cannot write to debug file");
        }

        BufferedReader settingsReader;
        if(settingsFileName.equals("-")) {
            settingsReader = new BufferedReader(new InputStreamReader(System.in, Charsets.UTF_8));
        } else {
            File settingsFile = new File(settingsFileName);
            try {
                settingsReader = Files.newReader(settingsFile, Charsets.UTF_8);
            } catch(FileNotFoundException e) {
                System.err.println("ERROR: Can't find settings file ("+ settingsFile + ")");
                return;
            }
        }

        try {
            settingsJson = CharStreams.toString(settingsReader);
        } catch(IOException e) {
            System.err.println("ERROR: Error while reading settings file");
            return;
        }

        OuterSettings outerSettings;
        try {
            outerSettings = gson.fromJson(settingsJson, OuterSettings.class);
        } catch(JsonSyntaxException e) {
            System.err.println("ERROR: Settings file is malformed!");
            return;
        }

        fullSettings = outerSettings.getSettings();

        dumperEngine = fullSettings.getDumperEngine();
        sourceEngine = fullSettings.getSourceEngine();
        if(dumperEngine == null) dumperEngine = "DumperJSON";
        if(sourceEngine == null) sourceEngine = "YotsubaJSON";

        for(String boardName : fullSettings.getBoardSettings().keySet()) {
            if("default".equals(boardName)) continue;
            try {
                spawnBoard(boardName, fullSettings);
            } catch(BoardInitException e) {
                System.err.println("ERROR: Error initializing dumper for /" + boardName + "/:");
                System.err.println("  " + e.getMessage());
            }
        }
    }
}
