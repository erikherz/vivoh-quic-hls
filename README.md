The goal is to deliver a standard HLS origin file set to a CDN and then a browser via QUIC / WebTransport. 

The browser will buffer the HLS data, start hls.js, and intercept server requests via XHR.

STATUS: Playback is broken.

Server Command:
```
./vqh-server --cert cert.pem --key key.pem
```

Publisher Command:
```
./vqh-publisher --input /path/to/hls --server https://your.vqd-server.com/live/pub
```

Encoder Command:
```
ffmpeg -re -stream_loop -1 -i adena.mp4 -vf "drawtext=text='Virginia\: %{gmtime}':fontsize=48:fontcolor=white:x=24:y=24" -c:v libx264 -preset ultrafast -tune zerolatency -g 30 -keyint_min 30 -sc_threshold 0 -b:v 3000k -c:a aac -b:a 128k -f hls -hls_time 1 -hls_list_size 5 -hls_flags delete_segments+independent_segments -hls_segment_type mpegts -force_key_frames "expr:gte(t,n_forced*1)" -hls_segment_filename "./out/chunk-%05d.ts" ./out/playlist.m3u8
```

Experimental Pipe Input Option with GPAC:

Publisher Command (no Encoder needed):

```
gpac -i adena.mp4:loop -f reframer:fps=30 ffenc:c=libx264:preset=fast:b=2000k:g=30:r=30 ffenc:c=aac:b=128k -o stdout:ext=mp4:frag:cdur=1:cmaf=chls:box=tfdt:mvex:split_mode=tracks:subs_sidx=0:!tsalign | /home/ubuntu/vivoh-quic-hls/target/release/vqd-publisher --pipe --server https://va01.wtmpeg.com/live/pub
```
