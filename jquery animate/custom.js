$(document).ready(function () {

    function changeMenuImage(menuItem) {
        var image = $(".menuImage");
        image.fadeOut('slow', function () {
            image.attr("src", "images/" + menuItem + ".jpg");
            image.fadeIn('fast');
        });

    }

    $(".menuItem").mouseenter(function () {
        console.log("Mouse entering");
        changeMenuImage(this.id);
    });

    $(".menuItem").mouseleave(function () {
        console.log("Mouse leaving");
        changeMenuImage("menu-standardMenuImage");
    });

});