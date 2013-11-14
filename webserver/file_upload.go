package webserver

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"hug/config"
	"hug/core/users"
	"hug/logs"
	"hug/utils"
	"hug/utils/imageresize"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

//var progressStore map[string]float32

const (
	FileUploadCode_None int8 = iota
	FileUploadCode_Failed
	FileUploadCode_PermissionDenied
)

const (
	uidEncryptSalt1 = "#$@&*dd=="
	uidEncryptSalt2 = "#$@^&*()=="
)

type FileUploadResPkt struct {
	Code int8   `json:"code"`
	Path string `json:"p"`
}

const (
	FileUploadImgPath          = "/upload/img/"
	FileUploadAvatarPath       = "/upload/avatar/"
	FileUploadVoicePath        = "/upload/voice/"
	FilePort                   = ":9091"
	FileGetImgPath             = "/img/"
	FileGetAvatarPath          = "/avatar/"
	FileGetVoicePath           = "/voice/"
	chatImageThumbnailSavePath = "thumbnail"
)

var avatarSavePath string
var voiceSavePath string
var chatImageSavePath string

func initFileUpload() {
	var webserviceConfigFilename string
	if runtime.GOOS == "windows" {
		webserviceConfigFilename += "config_webservice_win.json"
	} else if runtime.GOOS == "darwin" {
		webserviceConfigFilename += "config_webservice_darwin.json"
	} else {
		webserviceConfigFilename += "config_webservice_linux.json"
	}

	cfg, err := config.LoadConfigFile(utils.ApplicationPath() + "/" + webserviceConfigFilename)
	if err != nil {
		logs.Logger.Critical("Load config failed: ", err)
		os.Exit(100)
		return
	}
	avatarSavePath, err = cfg.GetString("avatar_save_path")
	if err != nil {
		logs.Logger.Critical("Load avatar save path failed: ", err)
		os.Exit(100)
		return
	}
	exist, err := isPathExists(avatarSavePath)
	if err != nil {
		logs.Logger.Critical("check if path ", avatarSavePath, " exist error: ", err)
		os.Exit(100)
		return
	}
	if !exist {
		logs.Logger.Critical("path ", avatarSavePath, " not exist")
		os.Exit(100)
		return
	}

	voiceSavePath, err = cfg.GetString("voice_save_path")
	if err != nil {
		logs.Logger.Critical("Load voice save path failed: ", err)
		os.Exit(100)
		return
	}
	exist, err = isPathExists(voiceSavePath)
	if err != nil {
		logs.Logger.Critical("check if path ", voiceSavePath, " exist error: ", err)
		os.Exit(100)
		return
	}
	if !exist {
		logs.Logger.Critical("path ", voiceSavePath, " not exist")
		os.Exit(100)
		return
	}

	chatImageSavePath, err = cfg.GetString("chat_image_save_path")
	if err != nil {
		logs.Logger.Critical("Load chat image save path failed: ", err)
		os.Exit(100)
		return
	}
	exist, err = isPathExists(chatImageSavePath)
	if err != nil {
		logs.Logger.Critical("check if path ", chatImageSavePath, " exist error: ", err)
		os.Exit(100)
		return
	}
	if !exist {
		logs.Logger.Critical("path ", chatImageSavePath, " not exist")
		os.Exit(100)
		return
	}

	// chatImageThumbnailSavePath = chatImageSavePath + "/thumbnail"
	// exist, err = isPathExists(chatImageThumbnailSavePath)
	// if err != nil {
	// 	logs.Logger.Critical("check if path ", chatImageThumbnailSavePath, " exist error: ", err)
	// 	os.Exit(100)
	// 	return
	// }
	// if !exist {
	// 	err = os.Mkdir(chatImageThumbnailSavePath, 0750)
	// 	if err != nil {
	// 		logs.Logger.Critical("make path ", chatImageThumbnailSavePath, " failed: ", err)
	// 		os.Exit(100)
	// 		return
	// 	}
	// }

	http.HandleFunc(FileUploadImgPath, handleImgUpload)
	http.HandleFunc(FileUploadAvatarPath, handleAvatarUpload)
	http.HandleFunc(FileUploadVoicePath, handleVoiceUpload)

	http.Handle(FileGetImgPath, http.StripPrefix(FileGetImgPath, http.FileServer(http.Dir(chatImageSavePath))))
	// http.Handle(FileGetThumbnailImgPath, http.StripPrefix(FileGetThumbnailImgPath, http.FileServer(http.Dir(chatImageThumbnailSavePath))))
	http.Handle(FileGetAvatarPath, http.StripPrefix(FileGetAvatarPath, http.FileServer(http.Dir(avatarSavePath))))
	http.Handle(FileGetVoicePath, http.StripPrefix(FileGetVoicePath, http.FileServer(http.Dir(voiceSavePath))))

	logs.Logger.Info("init file upload web server successful.")
	//progressStore = make(map[string]float32)
}

func handleAvatarUpload(w http.ResponseWriter, req *http.Request) {
	var resPkt FileUploadResPkt
	resPkt.Code = FileUploadCode_Failed
	resPkt.Path = ""
	defer func() {
		resData, err := json.Marshal(resPkt)
		if err != nil {
			logs.Logger.Critical("handleAvatarUpload json marshal respacket error:", err)
		}
		fmt.Fprint(w, string(resData))
	}()
	if req.Method == "POST" {
		req.ParseForm()
		logs.Logger.Info("New upload avatar request uid =", req.FormValue("uid"), "filename =", req.FormValue("filename"))
		uid, err := strconv.ParseInt(req.FormValue("uid"), 10, 64)
		if err != nil || uid == 0 {
			logs.Logger.Warn("handleAvatarUpload invalid uid:", uid, "err:", err)
			return
		}
		exist, err := users.IsUserInfoExist(uid)
		if err != nil {
			logs.Logger.Warn("handleAvatarUpload error: ", err)
			return
		}
		if !exist {
			logs.Logger.Warn("handleAvatarUpload error: uid=", uid, "not exist")
			return
		}

		filename := req.FormValue("filename")
		if len(filename) == 0 {
			logs.Logger.Warn("handleAvatarUpload error: invalid filename", filename)
			return
		}
		ext := filepath.Ext(filename)
		ext = strings.TrimPrefix(ext, ".")
		ext = strings.ToLower(ext)
		if ext != "jpg" && ext != "jpeg" && ext != "png" && ext != "gif" {
			logs.Logger.Warn("handleAvatarUpload error: invalid filetype:", ext)
			fmt.Fprint(w, fmt.Sprintln("file type:", ext, "not support"))
			return
		}

		token := req.FormValue("token")
		if uid == 0 || len(token) == 0 {
			logs.Logger.Warn("upload avatar err, no uid or token")
			return
		}
		uidstr := req.FormValue("uid")
		h := md5.New()
		io.WriteString(h, uidstr)
		io.WriteString(h, uidEncryptSalt1)
		io.WriteString(h, uidEncryptSalt2)
		token2 := fmt.Sprintf("%x", h.Sum(nil))
		if token != token2 {
			logs.Logger.Warn("upload avatar err, token mismatch")
			resPkt.Code = FileUploadCode_PermissionDenied
			return
		}

		tmpFile, _ := ioutil.TempFile(os.TempDir(), "upload-tmp-")
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			logs.Logger.Warn("handleAvatarUpload read post data err:", err)
		}
		tmpFile.Write(body)
		tmpFile.Close()
		fullPath := filepath.Join(avatarSavePath, filename)
		os.Remove(fullPath)
		os.Rename(tmpFile.Name(), fullPath)
		users.UpdateUserAvatar(uid, filename)
		logs.Logger.Info("handleAvatarUpload successful:", fullPath)
		resPkt.Code = FileUploadCode_None
	}
}

func handleImgUpload(w http.ResponseWriter, req *http.Request) {
	var resPkt FileUploadResPkt
	resPkt.Code = FileUploadCode_Failed
	resPkt.Path = ""
	defer func() {
		resData, err := json.Marshal(resPkt)
		if err != nil {
			logs.Logger.Critical("handleAvatarUpload json marshal respacket error:", err)
		}
		fmt.Fprint(w, string(resData))
	}()
	if req.Method == "POST" {
		req.ParseForm()
		logs.Logger.Info("New upload img request filename =", req.FormValue("filename"))

		filename := req.FormValue("filename")
		if len(filename) == 0 {
			logs.Logger.Warn("handleImgUpload error: invalid filename", filename)
			return
		}

		uid := req.FormValue("uid")
		token := req.FormValue("token")
		uidInt, err := strconv.Atoi(uid)
		fmt.Printf("%d", uidInt)
		if uidInt == 0 || err != nil || len(token) == 0 {
			fmt.Println(err)
			return
		}
		h := md5.New()
		io.WriteString(h, uid)
		io.WriteString(h, uidEncryptSalt1)
		io.WriteString(h, uidEncryptSalt2)
		token2 := fmt.Sprintf("%x", h.Sum(nil))
		if token != token2 {
			fmt.Println(token2)
			resPkt.Code = FileUploadCode_PermissionDenied
			return
		}

		ext := filepath.Ext(filename)
		ext = strings.TrimPrefix(ext, ".")
		ext = strings.ToLower(ext)
		if ext != "jpg" && ext != "jpeg" && ext != "png" && ext != "gif" {
			logs.Logger.Warn("handleImgUpload error: invalid filetype:", ext)
			fmt.Fprint(w, fmt.Sprintln("file type:", ext, "not support"))
			return
		}

		fileSavePath := chatImageSavePath
		retPath := makeDir(fileSavePath)
		fullPath := filepath.Join(fileSavePath, retPath, filename)
		if len(retPath) == 0 {
			resPkt.Path = ""
			resPkt.Code = FileUploadCode_Failed
			return
		}

		isExist := isFileExists(fullPath)
		if isExist {
			resPkt.Path = retPath
			resPkt.Code = FileUploadCode_None
			return
		}

		tmpFile, _ := ioutil.TempFile(os.TempDir(), "upload-tmp-")
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			logs.Logger.Warn("handleImgUpload read post data err:", err)
			return
		}
		tmpFile.Write(body)
		tmpFile.Close()
		os.Remove(fullPath)
		os.Rename(tmpFile.Name(), fullPath)
		generateThumbnailImg(filename, retPath)
		logs.Logger.Info("handleImgUpload successful:", fullPath)
		resPkt.Code = FileUploadCode_None
		resPkt.Path = retPath
	}
}

func generateThumbnailImg(filename string, path string) {
	source := filepath.Join(chatImageSavePath, path, filename)
	destDir := filepath.Join(chatImageSavePath, path, chatImageThumbnailSavePath)
	dest := filepath.Join(destDir, filename)

	err := os.Mkdir(destDir, os.ModeDir)
	if err != nil {
		logs.Logger.Warn("make thumbnail directory err", err)
	}
	sizeStr := "200w"

	file, err := os.Open(source)
	if err != nil {
		logs.Logger.Warn("generateThumbnailImg error:", err)
		return
	}
	defer file.Close()

	var img image.Image
	ext := ""

	img, err = jpeg.Decode(file)
	if err == nil {
		ext = "jpg"
	}

	if len(ext) == 0 {
		img, err = png.Decode(file)
		if err == nil {
			ext = "png"
		}
	}
	if len(ext) == 0 {
		img, err = gif.Decode(file)
		if err == nil {
			ext = "gif"
		}
	}
	if len(ext) == 0 {
		logs.Logger.Warn("error invalid image format")
		return
	}

	os.Remove(dest)
	outputImage := imageresize.Resize(img, sizeStr)
	fl, err := os.OpenFile(dest, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		logs.Logger.Critical("couldn't write", err)
		return
	}
	defer fl.Close()
	if ext == "jpg" {
		err = jpeg.Encode(fl, outputImage, nil)
		if err != nil {
			logs.Logger.Warn("error Encode image:", err)
			return
		}
	} else if ext == "png" || ext == "gif" {
		err = png.Encode(fl, outputImage)
		if err != nil {
			logs.Logger.Warn("error Encode image:", err)
			return
		}
	}
	logs.Logger.Info("save thumbnail img successful:", dest)
}

func handleVoiceUpload(w http.ResponseWriter, req *http.Request) {
	var resPkt FileUploadResPkt
	resPkt.Code = FileUploadCode_Failed
	defer func() {
		resData, err := json.Marshal(resPkt)
		if err != nil {
			logs.Logger.Critical("handleAvatarUpload json marshal respacket error:", err)
		}
		fmt.Fprint(w, string(resData))
	}()
	if req.Method == "POST" {
		req.ParseForm()
		logs.Logger.Info("New upload voice request filename =", req.FormValue("filename"))

		filename := req.FormValue("filename")
		if len(filename) == 0 {
			logs.Logger.Warn("handleVoiceUpload error: invalid filename", filename)
			return
		}

		uid := req.FormValue("uid")
		token := req.FormValue("token")
		uidInt, err := strconv.Atoi(uid)
		fmt.Printf("%d", uidInt)
		if uidInt == 0 || err != nil || len(token) == 0 {
			fmt.Println(err)
			return
		}
		h := md5.New()
		io.WriteString(h, uid)
		io.WriteString(h, uidEncryptSalt1)
		io.WriteString(h, uidEncryptSalt2)
		token2 := fmt.Sprintf("%x", h.Sum(nil))
		if token != token2 {
			fmt.Println(token2)
			resPkt.Code = FileUploadCode_PermissionDenied
			return
		}

		fileSavePath := voiceSavePath
		retPath := makeDir(fileSavePath)
		fullPath := filepath.Join(fileSavePath, retPath, filename)
		if len(retPath) == 0 {
			resPkt.Path = ""
			resPkt.Code = FileUploadCode_Failed
			return
		}

		isExist := isFileExists(fullPath)
		if isExist {
			resPkt.Path = retPath
			resPkt.Code = FileUploadCode_None
			return
		}

		tmpFile, _ := ioutil.TempFile(os.TempDir(), "upload-tmp-")
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			logs.Logger.Critical("handleVoiceUpload read post data err:", err)
			return
		}
		tmpFile.Write(body)
		tmpFile.Close()
		os.Remove(fullPath)
		os.Rename(tmpFile.Name(), fullPath)
		logs.Logger.Info("handleVoiceUpload successful:", fullPath)
		resPkt.Code = FileUploadCode_None
		resPkt.Path = retPath
	}
}

func makeDir(rootPath string) (path string) {
	path = time.Now().Format("2006/01/02")
	fullPath := filepath.Join(rootPath, path)
	err := os.MkdirAll(fullPath, os.ModePerm)
	if err != nil {
		logs.Logger.Critical("make dir error:", err)
		path = ""
	}
	return
}

func isPathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func isFileExists(path string) bool {
	info, err := os.Stat(path)
	if err == nil && !info.IsDir() {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}
